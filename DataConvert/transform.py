import pandas as pd
import logging

from .settings import LogMessages as LOGS

def prepare_mb51(df):
    logging.info(LOGS.specific_process)
    cols = ['Quantity', 'Amount in LC']
    def fix_values(x):
        x = x.replace('.','')
        x = x.replace(',','.')
        if x[-1]=='-':
            x = "-"+x[:-1]
        return float(x)
    df[cols] = df[cols].applymap(fix_values)
    df['Entry Date'] = df['Entry Date'].str.replace('.', '/', regex=False)
    df['Req. Date'] = df['Req. Date'].str.replace('.', '/', regex=False)
    df['Id'] = ''
    df['User Name'] = df['User Name'].astype('category').cat.codes
    final_cols = ['Id', 'Plnt', 'SLoc', 'Material', 'Quantity', 'MvT', 'Entry Date', 'Req. Date', 'Amount in LC', 'Reserv.no.']
    return df[final_cols]


def prepare_mb51mep(df):
    logging.info(LOGS.specific_process)
    cols = ['Quantity', 'Amount in LC']
    def fix_values(x):
        x = x.replace('.','')
        x = x.replace(',','.')
        if x[-1]=='-':
            x = "-"+x[:-1]
        return float(x)
    df[cols] = df[cols].applymap(fix_values)
    df['Entry Date'] = df['Entry Date'].str.replace('.', '/', regex=False)
    df['Req. Date'] = df['Req. Date'].str.replace('.', '/', regex=False)
    df['Id'] = ''
    final_cols = ['Id', 'Plnt', 'SLoc', 'Material', 'Quantity', 'MvT', 'Entry Date', 'Req. Date', 'Amount in LC', 'Reserv.No.']
    return df[final_cols]


def prepare_mb52(df):
    logging.info(LOGS.specific_process)
    new_cols = ['Plant', 'Warehouse', 'Material', 'Unrestricted', 'Blocked', 'InTrf', 'InTransit']
    df.columns = new_cols
    cols = ['Unrestricted', 'Blocked', 'InTrf', 'InTransit']
    def fix_values(x):
        x = x.replace('.','')
        x = x.replace(',','.')
        if x[-1]=='-':
            x = "-"+x[:-1]
        return float(x)
    df[cols] = df[cols].applymap(fix_values)
    return df


def prepare_zfi(df):
    logging.info(LOGS.specific_process)
    final_cols = ['From', 'To', 'Valid from', 'Exch. Rate']
    def fix_value(x):
        x = x.replace('.','')
        x = x.replace(',','.')
        return 1/float(x[1:])
    df['Exch. Rate'] = df['Exch. Rate'].apply(fix_value)
    df['Exch. Rate'] = df['Exch. Rate'] * df['Ratio (to)'] / df['Ratio (from)']
    df['Valid from'] = df['Valid from'].str.replace('.', '/', regex=False)
    return df[final_cols]


def prepare_zmb25(df):
    logging.info(LOGS.specific_process)
    def fix_values(x):
        x = x.replace('.','')
        x = x.replace(',','.')
        if x[-1]=='-':
            x = "-"+x[:-1]
        return float(x)
    cols1 = ['Del', 'FIs']
    cols2 = ['ReqmtsDate', 'Deliv.date', 'Base date']
    cols3 = ['Reqmnt qty', 'Diff. qty']
    df[cols3] = df[cols3].applymap(fix_values)
    df[cols1] = df[cols1].applymap(lambda x: 1 if x=='X' else 0)
    for col in cols2:
        df[col] = df[col].str.replace('.', '/', regex=False)
    df = df.drop(columns=['Full Name'])
    return df
    

def prepare_zmm001(df):
    logging.info(LOGS.specific_process)
    cols = ['Created', 'Last Chg']
    for col in cols:
        df[col] = df[col].str.replace('.', '/', regex=False)
    df = df.drop(columns=['ValA'])
    # 40319421 & 403119420 needed this
    df.loc[df['Last Chg']=='00/00/0000', 'Last Chg'] = '31/01/2020'
    df['Last Chg'] = pd.to_datetime(df['Last Chg'])
    df = df.sort_values('Last Chg').drop_duplicates('Material',keep='first')
    return df


def prepare_mcba(df):
    logging.info(LOGS.specific_process)
    df = df[(df['Matl type']=='ZMAT') | (df['Matl type']=='ZPEC')]
    final_cols = ['Plant', 'Material', 'Stor. loc.', 'MRP Type', 'Month', 'Val.stk(I)', 'Val.stk(R)', 'Val. stock', 'ValStckVal', 'VlStkRcVal', 'VlStkIssVl']
    df = df[final_cols]
    df = df[df['Material'].notna()]
    df['Stor. loc.'] = df['Stor. loc.'].astype(str).apply(lambda x: x.split('.0')[0])
    df['Plant'] = df['Plant'].astype(str).apply(lambda x: x.split('.0')[0])
    df['Month'] = df['Month'].astype(str).apply(lambda x: "01/"+x.split('.')[0]+"/"+x.split('.')[1] if int(x.split('.')[1]) > 202 else "01/"+x.split('.')[0]+"/2020")
    df.loc[df['Material'].isna(), 'Material'] = ''
    return df


def prepare_mrp(df):
    logging.info(LOGS.specific_process)
    def priority(x):
        if x=='@08@':
            return 'Low'
        elif x=='@0A@':
            return 'High'
        else:
            return 'Medium'
    df['PRIORITY'] = df['PRIORITY'].apply(priority)
    return df


def prepare_materialclasses(df):
    logging.info("Starting to process Material Classes")
    cols = ["PIC", "Material"]
    df.columns = cols
    return df