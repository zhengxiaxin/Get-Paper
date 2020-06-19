import pandas as pd
from datetime import date
from pandas import DataFrame
from pandas.tseries.offsets import BDay, Day


class GetPaper:
    df: DataFrame

    def __init__(self, file_path, file_name):
        self.file_path = file_path
        self.file_name = file_name
        self.file = self.file_path + self.file_name

    def get_data(self):
        self.df = pd.read_csv(self.file, encoding='gbk', engine='python')  # 读取csv文件
        self.df = pd.DataFrame(self.df).infer_objects()  # 类型变更为标准DataFrame
        self.df = self.df.infer_objects()  # 根据数据内容推断并转换数据格式

    def format_date(self):
        self.df['Trade Date'] = pd.to_datetime(self.df['Trade Date'],
                                               format='%Y/%m/%d')  # 交易日期改为datetime格式

    def screening_date(self):
        date_index = self.df[self.df['Trade Date'] == last_bis_day()].index.to_list()
        print(date_index)
        if len(date_index) == 0:
            print('上一个交易日无交易')
            return False
        else:
            date_min = min(date_index)
            date_max = max(date_index) + 1
            self.df = self.df[date_min:date_max]  # 筛选上一个工作日的交易
            return True

    def screening_book(self, book):
        self.df = self.df.loc[self.df["Strategy Book"] == book]  # 筛选账本
        return self.df


class Futures:

    def __init__(self, df):
        self.df = df

    def rearrange_columns(self):  # 根据目标表格重排列
        self.df = self.df[[
            'Trade Date', 'Trader', 'Type', 'C/P', 'Booking', 'Commodity', 'Volume', 'Qty UOM', 'Price',
            'Price UOM', 'Month', 'Pricing FM', 'Pricing TO'
        ]]

    def replace(self):
        trans = {  # 定义替换内容
            'Bullet Swap(Lookalike)': 'Bullet Swap',
            'PETROCHINA INTERNATIONAL (HONG KONG) CORPORATION LIMITED': 'PCI Take',
            'BOCI GLOBAL COMMODITIES LIMITED': 'BOCI',
            'ICBC STANDARD BANK PLC': 'ICBC Standard',
            'TTF_ICE': 'TTF',
            'Brent_ICE': 'Brent',
            'JKM_Platts': 'JKM',
            'WTI_Nymex': 'WTI',
            'NG_Henry Hub': 'HH',
        }
        self.df = self.df.replace(trans)

    def normalize_date(self):
        self.df['Month'] = pd.to_datetime(self.df['Month'],
                                          format='%b %y')  # 合约月改为datetime格式
        self.df['Month'] = self.df['Month'] + 14 * Day()  # 将合约月份日期移至每月15日
        self.df['Pricing FM'] = self.df['Pricing FM'].astype('M8[D]')  # 改为精确到日
        self.df['Pricing TO'] = self.df['Pricing TO'].astype('M8[D]')
        print(self.df)
        return self.df


class Options:

    def __init__(self, df):
        self.df = df

    def rearrange_columns(self):  # 根据目标表格重排列
        self.df = self.df[[
            'Trade Date', 'Trader', 'Booking', 'Volume', 'Qty UOM', 'Price', 'Price UOM',
            'S.Price UOM', 'Expire Date', 'Month', 'Commodity'
        ]]

    def replace(self):
        trans = {  # 定义替换内容
            'PETROCHINA INTERNATIONAL (HONG KONG) CORPORATION LIMITED': 'PCI Take',
            'TTF_ICE': 'TTF',
            'Brent_ICE': 'Brent',
            'JKM_Platts': 'JKM',
            'WTI_Nymex': 'WTI',
        }
        self.df = self.df.replace(trans)

    def normalize_date(self):
        self.df['Month'] = pd.to_datetime(self.df['Month'],
                                          format='%b %y')  # 合约月改为datetime格式
        self.df['Month'] = self.df['Month'] + 14 * Day()  # 将合约月份日期移至每月15日
        self.df['Expire Date'] = pd.to_datetime(self.df['Expire Date'],
                                                format="%b/%d/%Y")  # 到期日改为datetime格式
        return self.df

    @property
    def get_line(self):
        line: int = len(self.df['Month'])
        print('行数', line)
        return line

    def get_space(self, line):
        self.sp = {'space': [' '] * line}
        self.sp = pd.DataFrame(self.sp)  # 创建空格列
        print('空格', self.sp)
        return self.sp

    def get_description(self, sp):
        trans = {  # 定义替换内容
            'WTI_Nymex': 'NYMEX Light Sweet Crude Oil Options Electronic',
            'Brent_ICE': 'ICE Brent Crude Oil Options',
            'TTF_ICE': 'ICE Endex Endex Dutch TTF Natural Gas Options',
        }
        self.df = self.df.replace(trans)

        self.df = self.df[['Month', 'Strike Price', 'P/C', 'Commodity']]

        self.df['Strike Price'] = self.df['Strike Price'].round(decimals=0)
        self.df['Strike Price'] = self.df['Strike Price'].astype(str)
        self.df['Strike Price'] = self.df['Strike Price'].str.replace('0', '0.00')

        self.df['Description'] = self.df['Month'] + sp['space'] + self.df['Strike Price'] + sp['space'] + self.df[
            'P/C'] + sp['space'] + self.df['Commodity']
        self.df = self.df[['Description']]
        return self.df

    def insert_columns(self, df1, df2, sp):
        self.df = pd.concat([df1, df2], axis=1)  # 将Description列并入最后一行
        self.df = pd.concat([self.df, sp], axis=1)  # 加入空列
        self.df = self.df[[
            'Trade Date', 'Trader', 'Booking', 'Description', 'Volume', 'Qty UOM', 'Price', 'Price UOM', 'S.Price UOM',
            'space', 'space', 'space', 'space', 'space', 'space', 'space', 'space', 'space',  'Expire Date', 'Month',
            'Commodity'
        ]]
        return self.df


def last_bis_day():
    today = date.today()
    last_b_day = today - BDay(n=1)  # 生成上一个工作日
    print('上一个工作日为：', last_b_day)
    return last_b_day


def main():
    # Get FuturesData

    df_name: str = 'paper.csv'
    df_path = 'D:/Users/lenovo/Documents/公务/天然气/分析/'

    df = GetPaper(df_path, df_name)

    df.get_data()
    df.format_date()
    df.screening_date()

    if df.screening_date():
        df = df.screening_book(book='2020 Optimization 1')

        df = Futures(df)

        df.rearrange_columns()
        df.replace()
        df = df.normalize_date()

        print(df.info())
        print(df.head())

        df_name = 'get_futures.csv'
        df_file: str = df_path + df_name
        print('输出文件路径为:', df_path)
        df.to_csv(df_file)
    else:
        pass

    # Get OptionsData
    df_name: str = 'option.csv'

    df = GetPaper(df_path, df_name)

    df.get_data()
    df.format_date()
    df.screening_date()

    if df.screening_date():
        df = df.screening_book(book='2020 Optimization 1')

        df1 = Options(df)
        df1.rearrange_columns()
        df1.replace()
        df1 = df1.normalize_date()

        print('df1', df1.info())
        print(df1)

        df2 = Options(df)
        line = df2.get_line
        sp = df2.get_space(line=line)
        df2 = df2.get_description(sp=sp)

        print('df2', df2.info())
        print(df2)

        df = Options(df)
        df = df.insert_columns(df1=df1, df2=df2, sp=sp)

        pd.set_option('display.max_columns', None)
        print(df.info())
        print(df)

        '''df2_name = 'description.csv'
        df2_file: str = df_path + df2_name
        print('输出文件路径为:', df_path)
        df2['Description'].to_csv(df2_file)'''

        df1_name = 'get_option.csv'
        df_file: str = df_path + df1_name
        print('输出文件路径为:', df_path)
        df.to_csv(df_file)
    else:
        pass


if __name__ == '__main__':
    main()
