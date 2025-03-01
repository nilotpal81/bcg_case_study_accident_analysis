from pyspark.sql.functions import col, row_number
from pyspark.sql import Window
from src.utils import load_csv_data_to_df, write_output


class carCrashAnalysis:
    def __init__(self, spark, input_file_paths):

        self.df_charges = load_csv_data_to_df(spark, input_file_paths.get("Charges"))
        self.df_damages = load_csv_data_to_df(spark, input_file_paths.get("Damages"))
        self.df_endorse = load_csv_data_to_df(spark, input_file_paths.get("Endorse"))
        self.df_primary_person = load_csv_data_to_df(spark, input_file_paths.get("Primary_Person"))
        self.df_units = load_csv_data_to_df(spark, input_file_paths.get("Units"))
        self.df_restrict = load_csv_data_to_df(spark, input_file_paths.get("Restrict"))

    def count_male_accidents(self, output_path, output_format):
        """
        Finds the number of crashes (accidents) in which number of males killed are greater than 2
            Parameters:
            - output_path (str): The file path for the output file.
            - output_format (str): The file format for writing the output.
            Returns:
            - int: The count of crashes in which number of males killed are greater than 2
        """
        male_df = self.df_primary_person.filter(col("PRSN_GNDR_ID") == "MALE")
        male_death_count = male_df.groupBy("CRASH_ID")\
                                    .agg({"DEATH_CNT": "sum"})\
                                    .withColumnRenamed("sum(DEATH_CNT)", "Male_Deaths")
        filtered_crashes = male_death_count.filter(col("Male_Deaths") > 2)
        return filtered_crashes.count()

    def count_2_wheeler_accidents(self, output_path, output_format):
        """
        How many two wheelers are booked for crashes?

        Parameters:
        - output_format (str): The file format for writing the output.
        - output_path (str): The file path for the output file.
        Returns:
        - int: The count of crashes involving 2-wheeler vehicles.
        """
        df = self.df_units.filter(col("VEH_BODY_STYL_ID").contains("MOTORCYCLE"))
        write_output(df, output_path, output_format)

        return df.count()

    def top_5_vehicle_makes_for_fatal_crashes_without_airbags(
        self, output_path, output_format
    ):
        """
        Determines the top 5 Vehicle Makes of the cars involved in crashes where the driver died and airbags did not
        deploy.
        Parameters: - output_format (str): The file format for writing the output. - output_path (str): The
        file path for the output file.
        Returns: - List[str]: Top 5 vehicles Make for killed crashes without an airbag
        deployment.

        """
        df = (
            self.df_units.alias("u").join(self.df_primary_person.alias("p"),col("u.CRASH_ID")==col("p.CRASH_ID"),"inner")
            .filter(
                (col("p.PRSN_INJRY_SEV_ID") == "KILLED")
                & (col("p.PRSN_AIRBAG_ID") == "NOT DEPLOYED")
                & (~col("VEH_MAKE_ID").isin(['NA','UNKNOWN']))
            )
            .groupby("u.VEH_MAKE_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(5)
        )
        write_output(df, output_path, output_format)

        return df.select("VEH_MAKE_ID").rdd.flatMap(lambda row: row).collect()
        # return [row[0] for row in df.collect()]

    def count_hit_and_run_with_valid_licenses(self, output_path, output_format):
        """
        Determine number of Vehicles with driver having valid licences involved in hit and run?
        Parameters:
        - output_path (str): The file path for the output file.
        - output_format (str): The file format for writing the output.
        Returns:
        - int: The count of vehicles involved in hit-and-run incidents with drivers holding valid licenses.
        """
        df = (
            self.df_units.alias("u").select(col("u.CRASH_ID"), col("u.VEH_HNR_FL"))
                .join(self.df_primary_person.alias("p").select("p.CRASH_ID", "p.DRVR_LIC_TYPE_ID"), col("u.CRASH_ID") == col("p.CRASH_ID"),
                      "inner")
                .filter((col("VEH_HNR_FL") == "Y") & (
                col("DRVR_LIC_TYPE_ID").isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."])))
                .distinct()
        )
        df = df.select(col("p.CRASH_ID"),col("u.VEH_HNR_FL"))
        write_output(df, output_path, output_format)

        return df.count()

    def get_state_with_no_female_accident(self, output_path, output_format):
        """
        Which state has highest number of accidents in which females are not involved?
        Parameters:
        - output_format (str): The file format for writing the output.
        - output_path (str): The file path for the output file.
        Returns:
        - str: The state with the highest number of accidents without female involvement.
        """
        df = (
            self.df_primary_person.filter(
                (col("PRSN_GNDR_ID")!= "FEMALE") & (~col("DRVR_LIC_STATE_ID").isin("NA","Unknown","Other"))
            )
            .groupby("DRVR_LIC_STATE_ID")
            .count()
            .orderBy(col("count").desc())
        )
        write_output(df, output_path, output_format)

        return df.first().DRVR_LIC_STATE_ID

    def get_top_vehicle_contributing_to_injuries(self, output_path, output_format):
        """
        Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        Parameters: - output_format (str): The file format for writing the output. -
        output_path (str): The file path for the output file.
        Returns: - List[int]: The Top 3rd to 5th VEH_MAKE_IDs
        that contribute to the largest number of injuries, including death.
        """
        df = (
            self.df_units.filter(~col("VEH_MAKE_ID").isin(['NA','UNKNOWN']))
            .withColumn("TOT_CASUALTIES_CNT", col("TOT_INJRY_CNT") + col("DEATH_CNT"))
            .groupby("VEH_MAKE_ID")
            .sum("TOT_CASUALTIES_CNT")
            .withColumnRenamed("sum(TOT_CASUALTIES_CNT)", "TOT_CASUALTIES_CNT_AGG")
            .orderBy(col("TOT_CASUALTIES_CNT_AGG").desc())
        )

        df_top_3_to_5 = df.limit(5).subtract(df.limit(2))
        write_output(df_top_3_to_5, output_path, output_format)

        return [veh[0] for veh in df_top_3_to_5.select("VEH_MAKE_ID").collect()]

    def get_top_ethnic_ug_crash_for_each_body_style(self, output_path, output_format):
        """
        For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
        Parameters:
        - output_format (str): The file format for writing the output.
        - output_path (str): The file path for the output file.
        Returns:
        - dataframe
        """
        w = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())
        df = (
            self.df_units.join(self.df_primary_person, on=["CRASH_ID"], how="inner")
            .filter(
                ~self.df_units.VEH_BODY_STYL_ID.isin(
                    ["NA", "UNKNOWN", "NOT REPORTED", "OTHER  (EXPLAIN IN NARRATIVE)"]
                )
            )
            .filter(~self.df_primary_person.PRSN_ETHNICITY_ID.isin(["NA", "UNKNOWN"]))
            .groupby("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")
            .count()
            .withColumn("row", row_number().over(w))
            .filter(col("row") == 1)
            .drop("row", "count")
        )

        write_output(df, output_path, output_format)

        return df

    def get_top_5_zip_codes_with_alcohols_as_cf_for_crash(
        self, output_path, output_format
    ):
        """
        Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
        Parameters:
        - output_format (str): The file format for writing the output.
        - output_path (str): The file path for the output file.
        Returns:
        - List[str]: The top 5 Zip Codes with the highest number of alcohol-related crashes.

        """
        df = (
            self.df_units.join(self.df_primary_person, on=["CRASH_ID"], how="inner")
            .dropna(subset=["DRVR_ZIP"])
            .filter(
                col("CONTRIB_FACTR_1_ID").rlike("ALCOHOL|DRINKING")
                | col("CONTRIB_FACTR_2_ID").rlike("ALCOHOL|DRINKING")
            )
            .groupby("DRVR_ZIP")
            .count()
            .orderBy(col("count").desc())
            .limit(5)
        )
        write_output(df, output_path, output_format)

        return [row[0] for row in df.collect()]

    def get_crash_ids_with_no_damage(self, output_path, output_format):
        """
        Counts distinct Crash IDs where no damaged property was observed, the damage level (VEH_DMAG_SCL) is above 4,
        and the car has insurance.
        Parameters:
        - output_format (str): The file format for writing the output.
        - output_path (str): The file path for the output file.
        Returns:
        - List[str]: The list of distinct Crash IDs meeting the specified criteria.
        """
        df = (
            self.df_damages.join(self.df_units, on=["CRASH_ID"], how="inner")
            .filter(
                (
                    (self.df_units.VEH_DMAG_SCL_1_ID > "DAMAGED 4")
                    & (
                        ~self.df_units.VEH_DMAG_SCL_1_ID.isin(
                            ["NA", "NO DAMAGE", "INVALID VALUE"]
                        )
                    )
                )
                | (
                    (self.df_units.VEH_DMAG_SCL_2_ID > "DAMAGED 4")
                    & (
                        ~self.df_units.VEH_DMAG_SCL_2_ID.isin(
                            ["NA", "NO DAMAGE", "INVALID VALUE"]
                        )
                    )
                )
            )
            .filter(col("DAMAGED_PROPERTY").rlike("NONE|NONE1"))
            .filter(col("FIN_RESP_TYPE_ID").contains("INSURANCE"))
        )
        write_output(df, output_path, output_format)

        return df.count()

    def get_top_5_vehicle_brand(self, output_path, output_format):
        """
        Determines the top 5 Vehicle Makes/Brands where drivers are charged with speeding-related offences,
        have licensed drivers, use the top 10 used vehicle colours, and have cars licensed with the top 25 states
        with the highest number of offences.
        Parameters:
        - output_format (str): The file format for writing the output.
        - output_path (str): The file path for the output file.
        Returns: - List[str]: The list of top 5 Vehicle Makes/Brands meeting the specified criteria.
        """
        top_25_state_list = [
            row[0]
            for row in self.df_units.filter(col("VEH_LIC_STATE_ID")!='NA')
            .groupby("VEH_LIC_STATE_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(25)
            .collect()
        ]
        top_10_used_vehicle_colors = [
            row[0]
            for row in self.df_units.filter(self.df_units.VEH_COLOR_ID != "NA")
            .groupby("VEH_COLOR_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(10)
            .collect()
        ]

        df = (
            self.df_charges.join(self.df_primary_person, on=["CRASH_ID"], how="inner")
            .join(self.df_units, on=["CRASH_ID"], how="inner")
            .filter(self.df_charges.CHARGE.contains("SPEED"))
            .filter(
                self.df_primary_person.DRVR_LIC_TYPE_ID.isin(
                    ["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]
                )
            )
            .filter(self.df_units.VEH_COLOR_ID.isin(top_10_used_vehicle_colors))
            .filter(self.df_units.VEH_LIC_STATE_ID.isin(top_25_state_list))
            .groupby("VEH_MAKE_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(5)
        )

        write_output(df, output_path, output_format)

        return [row[0] for row in df.collect()]