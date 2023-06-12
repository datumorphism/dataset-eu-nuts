import requests
import json
import pandas as pd


ORDER_COLUMNS = [
    "nuts_code", "nuts_level", "country", "nuts_1", "nuts_2", "nuts_3"
]


def download_excel(data_remote, data_local, sheet, code_col, skiprows=None):

    df = pd.read_excel(data_remote, sheet_name=sheet, skiprows=skiprows)

    cols_drop = [i for i in df.columns.tolist() if i.startswith("Unnamed:")]
    df.drop(cols_drop, axis=1, inplace=True)

    if not code_col:
        raise Exception("Please specify the column name for the nuts code of the version")

    else:
        keep_cols = {
            code_col: "nuts_code",
            "Country": "country",
            "NUTS level 1": "nuts_1",
            "NUTS level 2": "nuts_2",
            "NUTS level 3": "nuts_3",
            "level": "nuts_level",
            "NUTS level": "nuts_level"
        }

        df = df.rename(columns=keep_cols)[
            list(set(keep_cols.values()))
        ][ORDER_COLUMNS]

    df = df.dropna(how="all")
    for c in ["nuts_code", "country", "nuts_1", "nuts_2", "nuts_3"]:
        df[c] = df[c].str.strip()
    df.to_csv(data_local, index=False)



def transform_to_json(raw, clean):

    df = pd.read_csv(raw)

    cols = {
        "nuts_code": "nuts_code",
        "Country": "country",
        "NUTS level 1": "nuts_1",
        "NUTS level 2": "nuts_2",
        "NUTS level 3": "nuts_3"
    }

    df_current = df.loc[~df["nuts_code"].isna()].rename(
        columns=cols
    )[cols.values()]

    json_data = {}

    for col in ["country", "nuts_1", "nuts_2", "nuts_3"]:
        df_current_col = df_current.loc[~df_current[col].isna()]
        df_current_col = df_current_col[["nuts_code", col]]
        df_current_col.rename(
            columns={
                "nuts_code": "code",
                col: "value"
            },
            inplace=True
        )
        json_data[col] = df_current_col.to_dict(orient="records")

    with open(clean, "w+", encoding='utf8') as fp:
        json.dump(json_data, fp, ensure_ascii=False)


NUTS_DATA = {
    "2021": {
        "remote": "https://ec.europa.eu/eurostat/documents/345175/629341/NUTS2021.xlsx",
        "sheet": "NUTS & SR 2021",
        "name": "v2021__2021_",
        "transform": {
            "code_column": "Code 2021"
        }
    },
    "2016": {
        "remote": "https://ec.europa.eu/eurostat/documents/345175/629341/NUTS2013-NUTS2016.xlsx",
        "sheet": "NUTS2013-NUTS2016",
        "name": "v2016__2018_2020",
        "skiprows": 1,
        "transform": {
            "code_column": "Code 2016"
        }
    },
    "2013": {
        "remote": "https://ec.europa.eu/eurostat/documents/345175/629341/NUTS+2010+-+NUTS+2013.xls",
        "sheet": "NUTS2010-NUTS2013",
        "name": "v2013__2015_2017",
        "skiprows": 1,
        "transform": {
            "code_column": "Code 2013"
        }
    },
    "2010": {
        "remote": "https://ec.europa.eu/eurostat/documents/345175/629341/2006-2010.xls",
        "sheet": "NUTS2006-NUTS2010",
        "name": "v2010__2012_2014",
        "skiprows": 1,
        "transform": {
            "code_column": "Code 2010"
        }
    },
    "2006": {
        "remote": "https://ec.europa.eu/eurostat/documents/345175/629341/2003-2006.xls",
        "sheet": "NUTS2003-NUTS2006",
        "name": "v2006__2008_2011",
        "skiprows": 1,
        "transform": {
            "code_column": "Code 2006"
        }
    },
    "2003": {
        "remote": "https://ec.europa.eu/eurostat/documents/345175/629341/1999-2003.xls",
        "sheet": "NUTS1999-NUTS2003",
        "name": "v2003__2004_2007",
        "skiprows": 1,
        "transform": {
            "code_column": "Code 2003"
        }
    }
}




if __name__ == "__main__":

    for nuts in NUTS_DATA:
        nuts_remote = NUTS_DATA[nuts]["remote"]
        nuts_sheet = NUTS_DATA[nuts]["sheet"]

        nuts_local_csv = f"dataset/nuts_{NUTS_DATA[nuts]['name']}.csv"
        nuts_local_json = f"dataset/nuts_{NUTS_DATA[nuts]['name']}.json"

        nuts_skiprows = NUTS_DATA[nuts].get("skiprows")

        nuts_transform = NUTS_DATA[nuts].get("transform")
        nuts_transform_code_col = nuts_transform.get("code_column")

        # Download Excel
        download_excel(
            nuts_remote,
            nuts_local_csv,
            nuts_sheet,
            nuts_transform_code_col,
            skiprows=nuts_skiprows
        )

        # Transform Data

        transform_to_json(
            nuts_local_csv,
            nuts_local_json
        )

    pass