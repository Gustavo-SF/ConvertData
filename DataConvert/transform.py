"""
Transformation module for each of the transactions
"""

import pandas as pd


def prepare_month_column(series: pd.Series) -> pd.Series:
    """Transform %m.%Y in %Y-%m format of month column"""
    series_fmt = series.apply(lambda x: f"{x.split('.')[1]}-{int(x.split('.')[0]):02}")
    return series_fmt


def fix_values(x: str) -> float:
    """Fix numeric values"""
    x = x.replace(".", "")
    x = x.replace(",", ".")
    if x[-1] == "-":
        x = "-" + x[:-1]
    return float(x)


# The following section has prepare functions for each of the transactions
# in case it is needed to make any specific change to one of them, we just
# need to update the function.
#
# They should always receive and return a dataframe.


def prepare_mb51(df):
    cols = ["quantity", "amount in lc"]
    df[cols] = df[cols].applymap(fix_values)
    df["entry date"] = df["entry date"].str.replace(".", "/", regex=False)
    df["req. date"] = df["req. date"].str.replace(".", "/", regex=False)
    df["id"] = ""
    df["user name"] = df["user name"].astype("category").cat.codes
    final_cols = [
        "id",
        "plnt",
        "sloc",
        "material",
        "quantity",
        "mvt",
        "entry date",
        "req. date",
        "amount in lc",
        "reserv.no.",
    ]
    return df[final_cols]


def prepare_mb52(df):
    new_cols = [
        "plant",
        "warehouse",
        "material",
        "unrestricted",
        "blocked",
        "intrf",
        "intransit",
    ]
    df.columns = new_cols
    cols = ["unrestricted", "blocked", "intrf", "intransit"]
    df[cols] = df[cols].applymap(fix_values)
    return df


def prepare_zfi(df):
    final_cols = ["from", "to", "valid from", "exch. rate"]

    def fix_value(x):
        x = x.replace(".", "")
        x = x.replace(",", ".")
        return 1 / float(x[1:])

    df["exch. rate"] = df["exch. rate"].apply(fix_value)
    df["exch. rate"] = (
        df["exch. rate"].astype(float)
        * df["ratio (to)"].astype(float)
        / df["ratio (from)"].astype(float)
    )
    df["valid from"] = df["valid from"].str.replace(".", "/", regex=False)
    return df[final_cols]


def prepare_zmb25(df):
    def fix_values(x):
        x = x.replace(".", "")
        x = x.replace(",", ".")
        if x[-1] == "-":
            x = "-" + x[:-1]
        return float(x)

    cols1 = ["del", "fis"]
    cols2 = ["reqmtsdate", "deliv.date", "base date"]
    cols3 = ["reqmnt qty", "diff. qty"]
    df[cols3] = df[cols3].applymap(fix_values)
    df[cols1] = df[cols1].applymap(lambda x: 1 if x == "X" else 0)
    for col in cols2:
        df[col] = df[col].str.replace(".", "/", regex=False)
    df = df.drop(columns=["full name"])
    return df


def prepare_zmm001(df):
    cols = ["created", "last chg"]
    for col in cols:
        df[col] = df[col].str.replace(".", "/", regex=False)
    df = df.drop(columns=["vala"])
    # 40319421 & 403119420 needed this
    df.loc[df["last chg"] == "00/00/0000", "last chg"] = "31/01/2020"
    df["last chg"] = pd.to_datetime(df["last chg"])
    df = df.sort_values("last chg").drop_duplicates("material", keep="first")
    return df


def prepare_mcba(df):
    final_cols = [
        "plant",
        "material",
        "stor. loc.",
        "mrp type",
        "month",
        "val.stk(i)",
        "val.stk(r)",
        "val. stock",
        "valstckval",
        "vlstkrcval",
        "vlstkissvl",
    ]
    df = df[final_cols]
    df = df[df["material"].notna()]
    df["stor. loc."] = df["stor. loc."].astype(str).apply(lambda x: x.split(".0")[0])
    df["plant"] = df["plant"].astype(str).apply(lambda x: x.split(".0")[0])
    df["month"] = prepare_month_column(df["month"])
    df.loc[df["material"].isna(), "material"] = ""
    return df


def prepare_mrp(df):
    def priority(x):
        if x == "@08@":
            return "Low"
        elif x == "@0A@":
            return "High"
        else:
            return "Medium"

    df["priority"] = df["priority"].apply(priority)
    return df


def prepare_materialclasses(df):
    cols = ["pic", "material"]
    df.columns = cols
    return df


def prepare_monos(df):
    return df


def prepare_sp99(df):
    return df
