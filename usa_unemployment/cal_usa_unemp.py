import pandas as pd
from jproperties import Properties
import sys


def main():
    configs = Properties()
    print("Trying to read config file...")
    try:
        with open("../resources/app-config.properties", "rb") as config_file:
            configs.load(config_file)
    except FileNotFoundError:
        print("property file not found", file=sys.stderr)
        return
    except PermissionError:
        print("Insufficient permission to read property file", file=sys.stderr)
        return

    print("Trying to read config properties...")

    # input_file is the original csv data file
    try:
        input_file = configs["input_file"]
    except KeyError as ke:
        print(f"{ke}, lookup key was input_file")
        return
    input_file = input_file.data
    print("input file name: " + input_file)

    # output_file is the extracted csv data file
    try:
        output_file = configs["output_file"]
    except KeyError as ke:
        print(f"{ke}, lookup key was output_file")
        return
    output_file = output_file.data
    print("output file name: " + output_file)

    # create an USAYouthUnemployment object
    usa_youth_unemp = USAYouthUnemployment(input_file, output_file)

    # call the __call__ function of the object for data ingestion work
    usa_youth_unemp()


class USAYouthUnemployment(object):
    def __init__(self, input_file, output_file):
        self.input_file = input_file
        self.output_file = output_file

    def __call__(self):
        """
        this function reads the original youth unemployment data in csv format, calculate the yearly unemployment
         and change every date for different cities and write it to another csv file.
        """

        # read original csv data
        print("Trying to read original csv data....")
        try:
            df = pd.read_csv(self.input_file)
        except FileNotFoundError as e:
            print(
                "Original data file: " + self.input_file + " not found!",
                file=sys.stderr,
            )
            return
        except pd.errors.EmptyDataError:
            print("No data found in original file: " + self.input_file, file=sys.stderr)
            return
        except pd.errors.ParserError:
            print(
                "Parse error in original data file: " + self.input_file, file=sys.stderr
            )
            return

        print("Top 10 rows of original data \n")
        print(df.head(10))

        # rename the year column to date
        df.rename(columns={"year": "date"}, inplace=True)

        # add a column tol_unemp as the total number of unemployed youth
        df["tol_unemp"] = (
            df["B23001_008E"]
            + df["B23001_015E"]
            + df["B23001_022E"]
            + df["B23001_094E"]
            + df["B23001_101E"]
            + df["B23001_108E"]
        )
        print("Top 10 rows of original data with tol_unemp added\n")
        print(df.head(10))

        # add a column tol_num_youth as the total number of youth
        df["tol_num_youth"] = (
            df["B23001_006E"]
            + df["B23001_013E"]
            + df["B23001_020E"]
            + df["B23001_092E"]
            + df["B23001_099E"]
            + df["B23001_106E"]
        )
        print("Top 10 rows of original data with tol_num_youth added \n")
        print(df.head(10))

        # add a column value as the youth unemployment rate
        df["value"] = df["tol_unemp"] / (df["tol_num_youth"])
        print("Top 10 rows of original data with value added \n")
        print(df.head(10))

        # select city, date and value from the data frame as extracted data
        df_extract = df[["city", "date", "value"]]
        print("Top 10 rows of extracted data \n")
        print(df_extract.head(10))

        # sort the extracted data by city and date
        df_extract = df_extract.sort_values(["city", "date"])
        print("Top 10 rows of extracted data \n")
        print(df_extract.head(10))

        # add a delta column as the unemployment change from the previous date grouped by city
        df_extract["delta"] = df_extract.groupby("city")["value"].diff()
        print("Top 10 rows of extracted data with delta added \n")
        print(df_extract.head(20))

        # write the data frame to the csv file
        print("Start writing to csv file: " + self.output_file)
        df_extract.to_csv(self.output_file, header=True, index=False)
        print("Finish writing to csv file")


if __name__ == "__main__":
    main()
