from src.utils import read_config
from src.car_crash_analysis import carCrashAnalysis
from utilities.spark_session import *
from utilities.logging_config import *


def main():
    spark = spark_session()

    # Set the Spark log level to suppress INFO and DEBUG logs
    spark.sparkContext.setLogLevel("ERROR")

    # Read configuration
    config_path = 'config/config.yaml'
    config = read_config(config_path)

    output_file_paths = config.get("OUTPUT_PATH")
    input_file_paths = config.get("INPUT_FILENAME")
    file_format = config.get("FILE_FORMAT")

    cca = carCrashAnalysis(spark, input_file_paths)

    try:
        logger.info("Fetching results for Analysis 1")
        # 1. Find the number of crashes (accidents) in which number of males killed are greater than 2?
        print(
            "1. Result:",
            cca.count_male_accidents(output_file_paths.get(1), file_format.get("Output")),
        )

        logger.info("Fetching results for Analysis 2")
        # 2. How many two-wheelers are booked for crashes?
        print(
            "2. Result:",
            cca.count_2_wheeler_accidents(
                output_file_paths.get(2), file_format.get("Output")
            ),
        )

        logger.info("Fetching results for Analysis 3")
        # 3. Determine the Top 5 Vehicles made of the cars present in the crashes in which a driver died and Airbags did
        # not deploy.
        print(
            "3. Result:",
            cca.top_5_vehicle_makes_for_fatal_crashes_without_airbags(
                output_file_paths.get(3), file_format.get("Output")
            ),
        )

        logger.info("Fetching results for Analysis 4")
        # 4. Determine the number of Vehicles with a driver having valid licences involved in hit-and-run?
        print(
            "4. Result:",
            cca.count_hit_and_run_with_valid_licenses(
                output_file_paths.get(4), file_format.get("Output")
            ),
        )

        logger.info("Fetching results for Analysis 5")
        # 5. Which state has the highest number of accidents in which females are not involved?
        print(
            "5. Result:",
            cca.get_state_with_no_female_accident(
                output_file_paths.get(5), file_format.get("Output")
            ),
        )

        logger.info("Fetching results for Analysis 6")
        # 6. Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        print(
            "6. Result:",
            cca.get_top_vehicle_contributing_to_injuries(
                output_file_paths.get(6), file_format.get("Output")
            ),
        )

        logger.info("Fetching results for Analysis 7")
        # 7. For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
        print("7. Result:")
        cca.get_top_ethnic_ug_crash_for_each_body_style(
            output_file_paths.get(7), file_format.get("Output")
        ).show(truncate=False)

        logger.info("Fetching results for Analysis 8")
        # 8. Among the crashed cars, what are the Top 5 Zip Codes with the highest number of crashes with alcohol as the
        # contributing factor to a crash (Use Driver Zip Code)
        print(
            "8. Result:",
            cca.get_top_5_zip_codes_with_alcohols_as_cf_for_crash(
                output_file_paths.get(8), file_format.get("Output")
            ),
        )

        logger.info("Fetching results for Analysis 9")
        # 9. Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above
        # 4 and car avails Insurance
        print(
            "9. Result:",
            cca.get_crash_ids_with_no_damage(
                output_file_paths.get(9), file_format.get("Output")
            ),
        )

        logger.info("Fetching results for Analysis 10")
        # 10. Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed
        # Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of
        # offenses (to be deduced from the data)
        print(
            "10. Result:",
            cca.get_top_5_vehicle_brand(
                output_file_paths.get(10), file_format.get("Output")
            ),
        )

    except Exception as e:
        # Log the exception
        logger.error(f"An error occurred: {e}")
        raise

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
