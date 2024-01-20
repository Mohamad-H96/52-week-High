import finpy_tse as tse
import pandas as pd
import numpy as np
import requests
from khayyam import JalaliDate
from bs4 import BeautifulSoup
import statsmodels.api as sm

# These Web_IDs were collected from the tsetmc website, and each represents an industry.
sector_web_id = [34408080767216529,19219679288446732,13235969998952202,62691002126902464,59288237226302898,69306841376553334,58440550086834602,30106839080444358,25766336681098389,\
     12331083953323969,36469751685735891,32453344048876642,1123534346391630,11451389074113298,33878047680249697,24733701189547084,20213770409093165,21948907150049163,40355846462826897,\
     54843635503648458,15508900928481581,3615666621538524,33626672012415176,65986638607018835,57616105980228781,70077233737515808,14651627750314021,34295935482222451,72002976013856737,\
     25163959460949732,24187097921483699,41867092385281437,61247168213690670,61985386521682984,4654922806626448,8900726085939949,18780171241610744,47233872677452574,65675836323214668,\
     59105676994811497]

# These are the names of different industries which are available in the tsetmc website.
sector_list = ['زراعت','ذغال سنگ','کانی فلزی','سایر معادن','منسوجات','محصولات چرمی','محصولات چوبی','محصولات کاغذی','انتشار و چاپ','فرآورده های نفتی','لاستیک',\
                   'فلزات اساسی','محصولات فلزی','ماشین آلات','دستگاه های برقی','وسایل ارتباطی','خودرو','قند و شکر','چند رشته ای','تامین آب، برق و گاز','غذایی',\
                   'دارویی','شیمیایی','خرده فروشی','کاشی و سرامیک','سیمان','کانی غیر فلزی','سرمایه گذاری','بانک','سایر مالی','حمل و نقل',\
                   'رادیویی','مالی','اداره بازارهای مالی','انبوه سازی','رایانه','اطلاعات و ارتباطات','فنی مهندسی','استخراج نفت','بیمه و بازنشستگی']

# This dictionary is needed for using requests.get method.
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

# This part of the URL is the same in all industries' URLs.
main_url = "http://tsetmc.com/Loader.aspx?ParTree=15131J&i="

#Converting Strings to JalaliDate Type
def to_jalali(df):
    """
Convert a dataframe column of dates in "YYYY-MM-DD" format to Jalali dates.

The function takes a dataframe as input and returns a list of Jalali dates.
The dates in the dataframe column should be in the format of "YYYY-MM-DD".

Input:
df: pandas DataFrame
Dataframe with a column of dates in "YYYY-MM-DD" format.

Returns:
dates: list of JalaliDate
A list of Jalali dates converted from the dates in the dataframe column.
"""
    dates = []
    for i in range(0, len(df)):
        d = df.iloc[:, 0][i].split("-")
        dates.append(JalaliDate(d[0], int(d[1]), int(d[2])))
    return dates

#Reading CSV File and Converting Date Column to Date Type
def read_data(name):
    """
Read a csv file and convert the first column of dates to Jalali format.

The function reads a csv file, converts the first column of dates from Gregorian to Jalali format using the "to_jalali" function, sets the first column as the index with the label "Date", and returns the resulting dataframe.

Input:
name: str
The name of the csv file to be read.

Returns:
df: pandas DataFrame
A dataframe with the first column of dates in Jalali format and set as the index with the label "Date".
"""
    df = pd.read_csv(name)
    df[df.columns[0]] = to_jalali(df)
    df.set_index(df.columns[0], inplace=True)
    df.index.name = "Date"
    return df


def sector_stocks():
    """
Get the list of stocks for each sector.

The function uses the "requests" library to access the webpages of sectors listed in "sector_list" and collects the names of stocks in each sector. The sector names and their corresponding stock names are stored in a dictionary and returned.

Input:
sector_list: list
A list of sector names.
sector_web_id: list
A list of web IDs for each sector.
main_url: str
The main URL for accessing the sector webpages.
headers: dict
A dictionary of headers for the HTTP request.

Returns:
Sectors: dict
A dictionary with sector names as keys and lists of stock names as values.
"""
    Sectors = {}
    for i in range(len(sector_list)):
        sector_url = main_url + str(sector_web_id[i])
        ww = []
        while len(ww) == 0:
            sector_page = requests.get(sector_url, headers=headers)
            soup = BeautifulSoup(sector_page.content, 'html.parser')
            ww = soup.find_all('a')
        stock_list = [ww[j].contents[0].replace("ي", "ی").replace("ك", "ک") for j in range(len(ww))]
        Sectors[sector_list[i]] = stock_list
    return Sectors

Sectors_stocks = sector_stocks()

def d2m(df):
    """
Extract the rows of a dataframe with unique months.

The function takes a dataframe with dates as the index and returns a new dataframe with only the rows that have unique months.

Input:
df: pandas DataFrame
A dataframe with dates as the index.

Returns:
df: pandas DataFrame
A dataframe with only the rows that have unique months in the index.
"""
    month_index = [df.index[0]]
    for i in range(1, len(df)-1):
        if df.index[i].month != df.index[i-1].month:
            month_index.append(df.index[i])
    month_index.append(df.index[-1])
    return df.loc[month_index, :]

def ret_d2m(df):
    """
Calculate the monthly returns of a dataframe.

The function takes a dataframe with daily values and returns a new dataframe with the monthly returns. The monthly returns are calculated by finding the product of the daily values for each unique month and subtracting 1.

Input:
df: pandas DataFrame
A dataframe with daily values.

Returns:
df2: pandas DataFrame
A dataframe with the monthly returns of the input dataframe. The index is set as the first day of each unique month.
"""
    df = df.copy(deep = True)
    df = df + 1
    df2 = pd.DataFrame(columns=df.columns)
    check = 0
    for i in range(1, len(df)):
        if (df.index[i].month != df.index[i-1].month) or (i == len(df) - 1):
            start = df.index[check]
            end = df.index[i]
            temp = df.loc[start:end, :]
            temp = temp.prod() - 1
            temp[temp == 0] = np.nan
            df2.loc[start, :] = temp
            check = i
    return df2

def Farvardin(df):
    """
Split a dataframe into two based on the month.

The function takes a dataframe with dates as the index and returns two dataframes, one with rows that have March as the month and another with the rest of the rows.

Input:
df: pandas DataFrame
A dataframe with dates as the index.

Returns:
farvardins: pandas DataFrame
A dataframe with the rows that have March as the month.
farvardin_excluded: pandas DataFrame
A dataframe with the rest of the rows.
"""
    dates = df.index
    farvardins = []
    farvardin_excluded = []
    for date in dates:
        if date.month == 1:
            farvardins.append(date)
        else:
            farvardin_excluded.append(date)

    return df.loc[farvardins, :], df.loc[farvardin_excluded, :]

##### JK Strategy #####

def JK_Ranker(ret, mc, J, t):
    """
Rank stocks based on their returns and market capitalization.

The function takes in stock returns and market capitalization data and uses them to rank the stocks. The ranking is done by grouping stocks into three categories: winners, losers, and middles.

The ranking is based on the average return of the stocks over the last J periods and the average market capitalization over the same period. The ranking is done on the most liquid stocks which are defined as those with a market capitalization greater than or equal to the 10th percentile of all market capitalizations in the same period.

Inputs:
ret: pandas DataFrame
A dataframe of returns for each stock, with time as the index.
mc: pandas DataFrame
A dataframe of market capitalization for each stock, with time as the index.
J: int
The number of periods over which to compute the average returns and market capitalization.
t: int
The current time period.

Returns:
winners: list of strings
A list of stocks that have the highest average return among the most liquid stocks over the last J periods.
losers: list of strings
A list of stocks that have the lowest average return among the most liquid stocks over the last J periods.
middles: list of strings
A list of stocks that have an average return that falls in between the highest and lowest returns among the most liquid stocks over the last J periods.
"""
    ret = ret.copy(deep=True)
    mc = mc.copy(deep = True)
    ret.index = range(0, len(ret))
    mc.index = range(0, len(mc))
    j_period_mc = mc.loc[t-J:t, :].dropna(axis = 1).mean().to_frame()
    liquids = j_period_mc[j_period_mc[0] >= j_period_mc.quantile(0.1)[0]].index
    j_period_return = ret.loc[t-J:t-1, liquids].dropna(axis = 1).mean().to_frame()
    winners = j_period_return[j_period_return[0] >= j_period_return.quantile(0.7)[0]].index
    losers = j_period_return[j_period_return[0] <= j_period_return.quantile(0.3)[0]].index
    middles = [stock for stock in liquids if (stock not in winners) and (stock not in losers)]
    return winners, losers, middles

def JK_Strategy(ret, mc, J, K, far = False):
    """
    JK_Strategy calculates the average returns for winners, losers, and the difference between winners and losers
    based on Jegadeesh and Titman (1993) momentum strategy. 

    Parameters
    ----------
    ret : pd.DataFrame
        A dataframe with stock returns.
    mc : pd.DataFrame
        A dataframe with market capitalization for each stock.
    J : int
        A lookback period for ranking the stocks.
    K : int
        A holding period for the strategy.
    far : bool, optional
        A flag to indicate whether Farvardin (1991) correction should be applied, by default False.

    Returns
    -------
    pd.DataFrame
        A dataframe with average returns for winners, losers, and the difference between winners and losers.
    """
    w_rets = []
    l_rets = []
    wl_rets = []
    if far:
        far_ret = Farvardin(ret)[0]
    for t in range(2 * J+1, len(ret)):
        w_ret = 0
        l_ret = 0
        wl_ret = 0
        winners, losers, middles = JK_Ranker(ret, mc, J, t)
        for i in range(t-K-1, t-1):
            if far:
                if ret.index[t] in far_ret.index:
                    w_ret += ret[winners].iloc[t, :].mean()
                    l_ret += ret[losers].iloc[t, :].mean()
                    wl_ret += ret[winners].iloc[t, :].mean() - ret[losers].iloc[t, :].mean()
            else:
                w_ret += ret[winners].iloc[t, :].mean()
                l_ret += ret[losers].iloc[t, :].mean()
                wl_ret += ret[winners].iloc[t, :].mean() - ret[losers].iloc[t, :].mean()
        w_rets.append(w_ret)
        l_rets.append(l_ret)
        wl_rets.append(wl_ret)
    Strategy = pd.DataFrame(index = ["JT's individual stock momentum"])
    Strategy["Winner"] = str(round(np.mean(w_rets) * 100, 2)) + "%"
    Strategy["Loser"] = str(round(np.mean(l_rets) * 100, 2)) + "%"
    Strategy["Winner - Loser"] = str(round(np.mean(wl_rets) * 100, 2)) + "%  (" + str(round(np.mean(wl_rets) * np.sqrt(len(wl_rets)) / np.std(wl_rets), 2)) + ")"
    Strategy.index.name = "Strategy"
    return Strategy

##### MG Strategy #####

def MG_Ranker(ind_ret, stocks_ret, J, t):
    """
MG_Ranker(ind_ret, stocks_ret, J, t)

Rank stocks into winners, losers, and middles based on their respective industry's mean return over the past J period.

Parameters:
ind_ret (pd.DataFrame): dataframe of industry returns
stocks_ret (pd.DataFrame): dataframe of stocks returns
J (int): number of periods used for ranking
t (int): current time step

Returns:
winners (list): list of winners' stocks
losers (list): list of losers' stocks
middles (list): list of stocks that are neither winners nor losers
"""
    ind_ret = ind_ret.copy(deep = True)
    ind_ret.index = range(0, len(ind_ret))
    j_period_return = ind_ret.loc[t-J:t-1, :].dropna(axis = 1, how = "all")
    j_period_return = j_period_return.mean().to_frame()
    winner_industries = j_period_return[j_period_return[0] >= j_period_return.quantile(0.7)[0]].index.tolist()
    loser_industries = j_period_return[j_period_return[0] <= j_period_return.quantile(0.3)[0]].index.tolist()
    winners = []
    losers = []
    for i in winner_industries:
        for j in Sectors_stocks[i]:
            winners.append(j)
    winners = list(set(winners))
    winners = [stock for stock in winners if stock in stocks_ret.columns]
    for i in loser_industries:
        for j in Sectors_stocks[i]:
            losers.append(j)
    losers = list(set(losers))
    losers = [stock for stock in losers if stock in stocks_ret.columns]
    middles = [stock for stock in stocks_ret.columns if (stock not in winners) and (stock not in losers)]
    return winners, losers, middles

def MG_Strategy(ind_ret, ret, J, K, far = False):
    """
This function implements the Momentum-Growth (MG) investment strategy by ranking industries based on their past returns and forming portfolios of winners and losers.

Parameters:
ind_ret (pandas DataFrame): DataFrame containing industry returns.
ret (pandas DataFrame): DataFrame containing asset returns.
J (int): Number of lookback periods used to rank the industries.
K (int): Number of holding periods.
far (bool): Whether to use Farvardin adjustment.

Returns:
Strategy (pandas DataFrame): DataFrame containing the average return for the winner and loser portfolios, and the difference between the two portfolios.

"""
    w_rets = []
    l_rets = []
    wl_rets = []
    if far:
        far_ret = Farvardin(ret)[0]
    for t in range(2*J+1, len(ret)):
        w_ret = 0
        l_ret = 0
        wl_ret = 0
        for i in range(t-K-1, t-1):
            if far:
                if ret.index[t] in far_ret.index:
                    winners, losers, middles = MG_Ranker(ind_ret, ret, J, i)
                    w_ret += ret[winners].iloc[t, :].mean()
                    l_ret += ret[losers].iloc[t, :].mean()
                    wl_ret += ret[winners].iloc[t, :].mean() - ret[losers].iloc[t, :].mean()
            else:
                winners, losers, middles = MG_Ranker(ind_ret, ret, J, i)
                w_ret += ret[winners].iloc[t, :].mean()
                l_ret += ret[losers].iloc[t, :].mean()
                wl_ret += ret[winners].iloc[t, :].mean() - ret[losers].iloc[t, :].mean()
        w_rets.append(w_ret)
        l_rets.append(l_ret)
        wl_rets.append(wl_ret)
    Strategy = pd.DataFrame(index = ["MG's industrial momentum"])
    Strategy["Winner"] = str(round(np.mean(w_rets) * 100, 2)) + "%"
    Strategy["Loser"] = str(round(np.mean(l_rets) * 100, 2)) + "%"
    Strategy["Winner - Loser"] = str(round(np.mean(wl_rets) * 100, 2)) + "%  (" + str(round(np.mean(wl_rets) * np.sqrt(len(wl_rets)) / np.std(wl_rets), 2)) + ")"
    Strategy.index.name = "Strategy"
    return Strategy

##### FT Strategy #####

def year_high(df):
    """
    Calculates the year-high of each stock in the input dataframe, `df`, and returns a dataframe containing the year-high of each stock.
    
    Parameters:
    df (pandas dataframe): A dataframe containing stock data.
    
    Returns:
    pandas dataframe: A dataframe containing the year-high of each stock.
    """
    first_index = df.index[0]
    start_index = len(df.loc[:JalaliDate(first_index.year + 1, first_index.month, first_index.day), :]) - 1
    result = df.iloc[start_index:, :].copy(deep = True)
    end_index = len(df)
    for i in range(start_index, end_index):
        end_date = df.index[i]
        start_date = JalaliDate(end_date.year - 1, end_date.month, end_date.day)
        max_of_year = df.loc[start_date:end_date].max().to_frame().transpose()
        max_of_year.index = [end_date]
        result.loc[end_date:end_date, :] = df.loc[end_date, :] / max_of_year
    return result

def FT_Ranker(df2, mc, quantile7, quantile3, i):
    """
    Ranks the stocks in `df2` based on their year-highs and returns the winners, losers, and middles (stocks that are not winners or losers) as three separate lists.
    
    Parameters:
    df2 (pandas dataframe): A dataframe containing the year-highs of the stocks.
    mc (pandas dataframe): A dataframe containing the market capitalization of the stocks.
    quantile7 (pandas series): A series containing the 70th percentile of the year-highs of the stocks.
    quantile3 (pandas series): A series containing the 30th percentile of the year-highs of the stocks.
    i (int): An integer representing the current time step.
    
    Returns:
    tuple: A tuple containing three lists, each containing the names of the winners, losers, and middles, respectively.
    """
    mc2 = mc.iloc[i-1:i, :]
    mc2 = mc2.dropna(axis = 1)
    q1 = mc2.quantile(0.1, axis = 1)
    mc2 = mc2.mean()
    liquids = mc2[mc2 >= q1[q1.index[0]]].index
    df3 = df2.iloc[i-1:i, :].loc[:, liquids]
    q7 = quantile7.loc[df2.index[i-1]]
    q3 = quantile3.loc[df2.index[i-1]]
    winners = df3.iloc[0, (df3 >= q7).values[0]].index
    losers = df3.iloc[0, (df3 <= q3).values[0]].index
    middles = [stock for stock in liquids if (stock not in winners) and (stock not in losers)]
    return winners, losers, middles

def FT_Strategy(df, ret, mc, J, K, t2 = False, far = False):
    """
    Implements a strategy based on ranking stocks based on their year-highs and returns a dataframe containing the performance of the strategy.
    
    Parameters:
    df (pandas dataframe): A dataframe containing stock data.
    ret (pandas dataframe): A dataframe containing the returns of the stocks.
    mc (pandas dataframe): A dataframe containing the market capitalization of the stocks.
    J (int): An integer representing the length of the moving average used in the strategy.
    K (int): An integer representing the number of previous time steps used in the strategy.
    t2 (bool): A boolean indicating whether or not to use the T2 version of the strategy.
    far (bool): A boolean indicating whether or not to use the Farvardin version of the strategy.
    
    Returns:
    pandas dataframe: A dataframe containing the performance of the strategy.
    """
    df = df.iloc[:, 1:].copy(deep = True)
    df2 = d2m(year_high(df)).iloc[:-1, :]
    if t2:
        if far:
            df2 = Farvardin(df2)[0]
        else:
            df2 = Farvardin(df2)[1]
    
    if far:
        far_ret = Farvardin(ret)[0]
    
    quantile7 = df2.quantile(0.7, axis = 1)
    quantile3 = df2.quantile(0.3, axis = 1)
    w_rets = []
    l_rets = []
    wl_rets = []
    for t in range(2*J+1, len(df2)):
        w_ret = 0
        l_ret = 0
        wl_ret = 0
        for i in range(t-K-1, t-1):
            if far:
                if ret.index[t] in far_ret.index:
                    winners, losers, middles = FT_Ranker(df2, mc, quantile7, quantile3, i)
                    w_ret += ret[winners].iloc[t, :].mean()
                    l_ret += ret[losers].iloc[t, :].mean()
                    wl_ret += ret[winners].iloc[t, :].mean() - ret[losers].iloc[t, :].mean()
            else:
                winners, losers, middles = FT_Ranker(df2, mc, quantile7, quantile3, i)
                w_ret += ret[winners].iloc[t, :].mean()
                l_ret += ret[losers].iloc[t, :].mean()
                wl_ret += ret[winners].iloc[t, :].mean() - ret[losers].iloc[t, :].mean()
        w_rets.append(w_ret)
        l_rets.append(l_ret)
        wl_rets.append(wl_ret)
    Strategy = pd.DataFrame(index = ["52-week high"])
    Strategy["Winner"] = str(round(np.mean(w_rets) * 100, 2)) + "%"
    Strategy["Loser"] = str(round(np.mean(l_rets) * 100, 2)) + "%"
    Strategy["Winner - Loser"] = str(round(np.mean(wl_rets) * 100, 2)) + "%  (" + str(round(np.mean(wl_rets) * np.sqrt(len(wl_rets)) / np.std(wl_rets), 2)) + ")"
    Strategy.index.name = "Strategy"
    return Strategy

def JT_FT(df, ret, mc, J, K):
    """
    This function implements the JT_FT strategy, which is a combination of the JK_Ranker and FT_Strategy functions. The JK_Ranker function sorts stocks into winners, losers, and middles groups based on their returns and market capitalization. The FT_Strategy function then implements a 6/6 momentum strategy for each group. The returns for each group and the combined "winners minus losers" group are calculated and stored in separate lists. The function then repeats the process for a Farvardin-adjusted version of the returns and market capitalization values. The final result is a dictionary of lists, containing the returns for each group as well as the combined "winners minus losers" group, both for the original and Farvardin-adjusted data.

    Parameters:
    df (DataFrame): The input dataframe containing the stocks' data.
    ret (Series): The returns of the stocks.
    mc (Series): The market capitalization of the stocks.
    J (int): The number of months to look back for ranking the stocks.
    K (int): The number of months to hold the stocks.

    Returns:
    dict: A dictionary of lists, containing the returns for each group as well as the combined "winners minus losers" group, both for the original and Farvardin-adjusted data.
    """
    ww_return = []
    wl_return = []
    wp_return = []
    mw_return = []
    ml_return = []
    mp_return = []
    lw_return = []
    ll_return = []
    lp_return = []
    for t in range(2*J+1, len(ret)):
        winners, losers, middles = JK_Ranker(ret, mc, J, t)
        #JT Winners
        mix_strategy = FT_Strategy(df[["Index"] + winners.tolist()], ret[winners], mc[winners], 6, 6)
        ww_return.append(mix_strategy["Winners"][0])
        wl_return.append(mix_strategy["Losers"][0])
        wp_return.append(mix_strategy["Winners - Losers"][0])

        #JT Middles
        mix_strategy = FT_Strategy(df[["Index"] + middles], ret[middles], mc[middles], 6, 6)
        mw_return.append(mix_strategy["Winners"][0])
        ml_return.append(mix_strategy["Losers"][0])
        mp_return.append(mix_strategy["Winners - Losers"][0])

        #JT Losers
        mix_strategy = FT_Strategy(df[["Index"] + losers.tolist()], ret[losers], mc[losers], 6, 6)
        lw_return.append(mix_strategy["Winners"][0])
        ll_return.append(mix_strategy["Losers"][0])
        lp_return.append(mix_strategy["Winners - Losers"][0])
    
    
    ret3 = Farvardin(ret)[1]
    mc3 = Farvardin(mc)[1]
    fww_return = []
    fwl_return = []
    fwp_return = []
    fmw_return = []
    fml_return = []
    fmp_return = []
    flw_return = []
    fll_return = []
    flp_return = []
    for t in range(2*J+1, len(ret3)):
        winners, losers, middles = JK_Ranker(ret3, mc3, J, t)
        #JT Winners
        mix_strategy = FT_Strategy(df[["Index"] + winners.tolist()], ret3[winners], mc3[winners], 6, 6, t2=True)
        fww_return.append(mix_strategy["Winners"][0])
        fwl_return.append(mix_strategy["Losers"][0])
        fwp_return.append(mix_strategy["Winners - Losers"][0])

        #JT Middles
        mix_strategy = FT_Strategy(df[["Index"] + middles], ret3[middles], mc3[middles], 6, 6, t2=True)
        fmw_return.append(mix_strategy["Winners"][0])
        fml_return.append(mix_strategy["Losers"][0])
        fmp_return.append(mix_strategy["Winners - Losers"][0])

        #JT Losers
        mix_strategy = FT_Strategy(df[["Index"] + losers.tolist()], ret3[losers], mc3[losers], 6, 6, t2=True)
        flw_return.append(mix_strategy["Winners"][0])
        fll_return.append(mix_strategy["Losers"][0])
        flp_return.append(mix_strategy["Winners - Losers"][0])
    
    for z in range(len(ww_return)):
        ww_return[z] = float(ww_return[z].split("%")[0]) / 100
        wl_return[z] = float(wl_return[z].split("%")[0]) / 100
        wp_return[z] = float(wp_return[z].split("%")[0]) / 100
        mw_return[z] = float(mw_return[z].split("%")[0]) / 100
        ml_return[z] = float(ml_return[z].split("%")[0]) / 100
        mp_return[z] = float(mp_return[z].split("%")[0]) / 100
        lw_return[z] = float(lw_return[z].split("%")[0]) / 100
        ll_return[z] = float(ll_return[z].split("%")[0]) / 100
        lp_return[z] = float(lp_return[z].split("%")[0]) / 100
        fww_return[z] = float(fww_return[z].split("%")[0]) / 100
        fwl_return[z] = float(fwl_return[z].split("%")[0]) / 100
        fwp_return[z] = float(fwp_return[z].split("%")[0]) / 100
        fmw_return[z] = float(fmw_return[z].split("%")[0]) / 100
        fml_return[z] = float(fml_return[z].split("%")[0]) / 100
        fmp_return[z] = float(fmp_return[z].split("%")[0]) / 100
        flw_return[z] = float(flw_return[z].split("%")[0]) / 100
        fll_return[z] = float(fll_return[z].split("%")[0]) / 100
        flp_return[z] = float(flp_return[z].split("%")[0]) / 100
    
    Mix_Strategy = pd.DataFrame(columns=["Portfolio Classified by FT", "Ave. Monthly Return", "Ave. Monthly Return Excluding Farvardin"],
                                index=["Winner", "Winner", "Winner", "Middle", "Middle", "Middle", "Loser", "Loser", "Loser"])
    Mix_Strategy.index.name = "Portfolio Classified by JT"
    Mix_Strategy.iloc[[0, 3, 6], 0] = "Winner"
    Mix_Strategy.iloc[[1, 4, 7], 0] = "Loser"
    Mix_Strategy.iloc[[2, 5, 8], 0] = "Winner - Loser"
    Mix_Strategy.iloc[0, 1] = str(round(np.mean(ww_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[1, 1] = str(round(np.mean(wl_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[2, 1] = str(round(np.mean(wp_return) * 100, 2)) + "% (" + str(round(np.mean(wp_return) * np.sqrt(len(wp_return)) / np.std(wp_return), 2)) + ")"
    Mix_Strategy.iloc[3, 1] = str(round(np.mean(mw_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[4, 1] = str(round(np.mean(ml_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[5, 1] = str(round(np.mean(mp_return) * 100, 2)) + "% (" + str(round(np.mean(mp_return) * np.sqrt(len(mp_return)) / np.std(mp_return), 2)) + ")"
    Mix_Strategy.iloc[6, 1] = str(round(np.mean(lw_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[7, 1] = str(round(np.mean(ll_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[8, 1] = str(round(np.mean(lp_return) * 100, 2)) + "% (" + str(round(np.mean(lp_return) * np.sqrt(len(lp_return)) / np.std(lp_return), 2)) + ")"
    Mix_Strategy.iloc[0, 2] = str(round(np.mean(fww_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[1, 2] = str(round(np.mean(fwl_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[2, 2] = str(round(np.mean(fwp_return) * 100, 2)) + "% (" + str(round(np.mean(fwp_return) * np.sqrt(len(fwp_return)) / np.std(fwp_return), 2)) + ")"
    Mix_Strategy.iloc[3, 2] = str(round(np.mean(fmw_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[4, 2] = str(round(np.mean(fml_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[5, 2] = str(round(np.mean(fmp_return) * 100, 2)) + "% (" + str(round(np.mean(fmp_return) * np.sqrt(len(fmp_return)) / np.std(fmp_return), 2)) + ")"
    Mix_Strategy.iloc[6, 2] = str(round(np.mean(flw_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[7, 2] = str(round(np.mean(fll_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[8, 2] = str(round(np.mean(flp_return) * 100, 2)) + "% (" + str(round(np.mean(flp_return) * np.sqrt(len(flp_return)) / np.std(flp_return), 2)) + ")"
    return Mix_Strategy

def FT_JT(df, ret, mc, J, K):
    """
    This function implements the Winner, Loser, and Winner - Loser (Mix) strategies on Farvardin and Tarsim rankings of financial time series data.

    Parameters:
    df (pd.DataFrame): The dataframe of financial time series data.
    ret (pd.DataFrame): The dataframe of returns of financial time series data.
    mc (pd.DataFrame): The dataframe of market capitalization of financial time series data.
    J (int): The number of periods for ranking the financial time series data.
    K (int): The number of periods for holding the position in the financial time series data.

    Returns:
    dict: A dictionary with the following keys:
        "Farvardin Winners": The average return of the Winner strategy in Farvardin ranking.
        "Farvardin Losers": The average return of the Loser strategy in Farvardin ranking.
        "Farvardin Winners - Losers": The average return of the Mix strategy in Farvardin ranking.
        "Farvardin Middles": The average return of the Middle strategy in Farvardin ranking.
        "Tarsim Winners": The average return of the Winner strategy in Tarsim ranking.
        "Tarsim Losers": The average return of the Loser strategy in Tarsim ranking.
        "Tarsim Winners - Losers": The average return of the Mix strategy in Tarsim ranking.
        "Tarsim Middles": The average return of the Middle strategy in Tarsim ranking.
    """
    df = df.iloc[:, 1:].copy(deep = True)
    df2 = d2m(year_high(df)).iloc[:-1, :]
    quantile7 = df2.quantile(0.7, axis = 1)
    quantile3 = df2.quantile(0.3, axis = 1)
    ww_return = []
    wl_return = []
    wp_return = []
    mw_return = []
    ml_return = []
    mp_return = []
    lw_return = []
    ll_return = []
    lp_return = []
    for i in range(100, 110):#range(2*J+1, len(ret)):
        winners, losers, middles = FT_Ranker(df2, mc, quantile7, quantile3, i)
        #FT Winners
        mix_strategy = JK_Strategy(ret[winners], mc[winners], J, K)
        ww_return.append(mix_strategy["Winners"][0])
        wl_return.append(mix_strategy["Losers"][0])
        wp_return.append(mix_strategy["Winners - Losers"][0])

        #FT Middles
        mix_strategy = JK_Strategy(ret[middles], mc[middles], J, K)
        mw_return.append(mix_strategy["Winners"][0])
        ml_return.append(mix_strategy["Losers"][0])
        mp_return.append(mix_strategy["Winners - Losers"][0])

        #FT Losers
        mix_strategy = JK_Strategy(ret[losers], mc[losers], J, K)
        lw_return.append(mix_strategy["Winners"][0])
        ll_return.append(mix_strategy["Losers"][0])
        lp_return.append(mix_strategy["Winners - Losers"][0])
    
    ret3 = Farvardin(ret)[1]
    mc3 = Farvardin(mc)[1]
    df2 = Farvardin(df2)[1]
    quantile7 = df2.quantile(0.7, axis = 1)
    quantile3 = df2.quantile(0.3, axis = 1)
    fww_return = []
    fwl_return = []
    fwp_return = []
    fmw_return = []
    fml_return = []
    fmp_return = []
    flw_return = []
    fll_return = []
    flp_return = []
    for i in range(100, 110):#range(2*J+1, len(ret)):
        winners, losers, middles = FT_Ranker(df2, mc, quantile7, quantile3, i)
        #FT Winners
        mix_strategy = JK_Strategy(ret3[winners], mc3[winners], J, K)
        fww_return.append(mix_strategy["Winners"][0])
        fwl_return.append(mix_strategy["Losers"][0])
        fwp_return.append(mix_strategy["Winners - Losers"][0])

        #FT Middles
        mix_strategy = JK_Strategy(ret3[middles], mc3[middles], J, K)
        fmw_return.append(mix_strategy["Winners"][0])
        fml_return.append(mix_strategy["Losers"][0])
        fmp_return.append(mix_strategy["Winners - Losers"][0])

        #FT Losers
        mix_strategy = JK_Strategy(ret3[losers], mc3[losers], J, K)
        flw_return.append(mix_strategy["Winners"][0])
        fll_return.append(mix_strategy["Losers"][0])
        flp_return.append(mix_strategy["Winners - Losers"][0])
    
    for z in range(len(ww_return)):
        ww_return[z] = float(ww_return[z].split("%")[0]) / 100
        wl_return[z] = float(wl_return[z].split("%")[0]) / 100
        wp_return[z] = float(wp_return[z].split("%")[0]) / 100
        mw_return[z] = float(mw_return[z].split("%")[0]) / 100
        ml_return[z] = float(ml_return[z].split("%")[0]) / 100
        mp_return[z] = float(mp_return[z].split("%")[0]) / 100
        lw_return[z] = float(lw_return[z].split("%")[0]) / 100
        ll_return[z] = float(ll_return[z].split("%")[0]) / 100
        lp_return[z] = float(lp_return[z].split("%")[0]) / 100
        fww_return[z] = float(fww_return[z].split("%")[0]) / 100
        fwl_return[z] = float(fwl_return[z].split("%")[0]) / 100
        fwp_return[z] = float(fwp_return[z].split("%")[0]) / 100
        fmw_return[z] = float(fmw_return[z].split("%")[0]) / 100
        fml_return[z] = float(fml_return[z].split("%")[0]) / 100
        fmp_return[z] = float(fmp_return[z].split("%")[0]) / 100
        flw_return[z] = float(flw_return[z].split("%")[0]) / 100
        fll_return[z] = float(fll_return[z].split("%")[0]) / 100
        flp_return[z] = float(flp_return[z].split("%")[0]) / 100
    
    Mix_Strategy = pd.DataFrame(columns=["Portfolio Classified by JT", "Ave. Monthly Return", "Ave. Monthly Return Excluding Farvardin"],
                                index=["Winner", "Winner", "Winner", "Middle", "Middle", "Middle", "Loser", "Loser", "Loser"])
    Mix_Strategy.index.name = "Portfolio Classified by FT"
    Mix_Strategy.iloc[[0, 3, 6], 0] = "Winner"
    Mix_Strategy.iloc[[1, 4, 7], 0] = "Loser"
    Mix_Strategy.iloc[[2, 5, 8], 0] = "Winner - Loser"
    Mix_Strategy.iloc[0, 1] = str(round(np.mean(ww_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[1, 1] = str(round(np.mean(wl_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[2, 1] = str(round(np.mean(wp_return) * 100, 2)) + "% (" + str(round(np.mean(wp_return) * np.sqrt(len(wp_return)) / np.std(wp_return), 2)) + ")"
    Mix_Strategy.iloc[3, 1] = str(round(np.mean(mw_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[4, 1] = str(round(np.mean(ml_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[5, 1] = str(round(np.mean(mp_return) * 100, 2)) + "% (" + str(round(np.mean(mp_return) * np.sqrt(len(mp_return)) / np.std(mp_return), 2)) + ")"
    Mix_Strategy.iloc[6, 1] = str(round(np.mean(lw_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[7, 1] = str(round(np.mean(ll_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[8, 1] = str(round(np.mean(lp_return) * 100, 2)) + "% (" + str(round(np.mean(lp_return) * np.sqrt(len(lp_return)) / np.std(lp_return), 2)) + ")"
    Mix_Strategy.iloc[0, 2] = str(round(np.mean(fww_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[1, 2] = str(round(np.mean(fwl_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[2, 2] = str(round(np.mean(fwp_return) * 100, 2)) + "% (" + str(round(np.mean(fwp_return) * np.sqrt(len(fwp_return)) / np.std(fwp_return), 2)) + ")"
    Mix_Strategy.iloc[3, 2] = str(round(np.mean(fmw_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[4, 2] = str(round(np.mean(fml_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[5, 2] = str(round(np.mean(fmp_return) * 100, 2)) + "% (" + str(round(np.mean(fmp_return) * np.sqrt(len(fmp_return)) / np.std(fmp_return), 2)) + ")"
    Mix_Strategy.iloc[6, 2] = str(round(np.mean(flw_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[7, 2] = str(round(np.mean(fll_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[8, 2] = str(round(np.mean(flp_return) * 100, 2)) + "% (" + str(round(np.mean(flp_return) * np.sqrt(len(flp_return)) / np.std(flp_return), 2)) + ")"
    return Mix_Strategy

def MG_FT(df, ind_ret, stocks_ret, mc, J, K):
    """
This function returns returns a number of strategies for a given data.

It starts with dividing stocks into three categories "Winners", "Middles", and "Losers" using the function MG_Ranker with the input ind_ret, stocks_ret and J. The resulting stocks of each category are then passed to the function FT_Strategy to get returns for a "Winner-Winner" strategy, "Winner-Loser" strategy, and "Winner-Winner-Loser" strategy for each category. The process is repeated after the stocks_ret and mc are transformed using Farvardin function.

Finally, the returns are converted to float and divided by 100 to get decimal form returns.

Inputs:
df: A pandas DataFrame, containing all the stocks and other information.
ind_ret: A pandas Series, containing returns of the index.
stocks_ret: A pandas DataFrame, containing returns of the stocks.
mc: A pandas Series, containing market capitalization of the stocks.
J: Integer, number of top performers to be considered as "winners".
K: Integer, number of bottom performers to be considered as "losers".

Returns:
Returns a number of lists containing returns of different strategies.
The lists are:
- ww_return
- wl_return
- wp_return
- mw_return
- ml_return
- mp_return
- lw_return
- ll_return
- lp_return
- fww_return
- fwl_return
- fwp_return
- fmw_return
- fml_return
- fmp_return
- flw_return
- fll_return
- flp_return
"""
    ww_return = []
    wl_return = []
    wp_return = []
    mw_return = []
    ml_return = []
    mp_return = []
    lw_return = []
    ll_return = []
    lp_return = []
    for t in range(100, 115):
        winners, losers, middles = MG_Ranker(ind_ret, stocks_ret, J, t)
        #MG Winners
        mix_strategy = FT_Strategy(df[["Index"] + winners], stocks_ret[winners], mc[winners], 6, 6)
        ww_return.append(mix_strategy["Winners"][0])
        wl_return.append(mix_strategy["Losers"][0])
        wp_return.append(mix_strategy["Winners - Losers"][0])

        #MG Middles
        mix_strategy = FT_Strategy(df[["Index"] + middles], stocks_ret[middles], mc[middles], 6, 6)
        mw_return.append(mix_strategy["Winners"][0])
        ml_return.append(mix_strategy["Losers"][0])
        mp_return.append(mix_strategy["Winners - Losers"][0])

        #MG Losers
        mix_strategy = FT_Strategy(df[["Index"] + losers], stocks_ret[losers], mc[losers], 6, 6)
        lw_return.append(mix_strategy["Winners"][0])
        ll_return.append(mix_strategy["Losers"][0])
        lp_return.append(mix_strategy["Winners - Losers"][0])
    
    stocks_ret3 = Farvardin(ret)[1]
    mc3 = Farvardin(mc)[1]
    ind_ret3 = Farvardin(ind_ret)[1]
    fww_return = []
    fwl_return = []
    fwp_return = []
    fmw_return = []
    fml_return = []
    fmp_return = []
    flw_return = []
    fll_return = []
    flp_return = []
    for t in range(100, 115):
        winners, losers, middles = MG_Ranker(ind_ret3, stocks_ret3, J, t)
        #MG Winners
        mix_strategy = FT_Strategy(df[["Index"] + winners], stocks_ret3[winners], mc3[winners], 6, 6, t2 = True)
        fww_return.append(mix_strategy["Winners"][0])
        fwl_return.append(mix_strategy["Losers"][0])
        fwp_return.append(mix_strategy["Winners - Losers"][0])

        #MG Middles
        mix_strategy = FT_Strategy(df[["Index"] + middles], stocks_ret3[middles], mc3[middles], 6, 6, t2 = True)
        fmw_return.append(mix_strategy["Winners"][0])
        fml_return.append(mix_strategy["Losers"][0])
        fmp_return.append(mix_strategy["Winners - Losers"][0])

        #MG Losers
        mix_strategy = FT_Strategy(df[["Index"] + losers], stocks_ret3[losers], mc3[losers], 6, 6, t2 = True)
        flw_return.append(mix_strategy["Winners"][0])
        fll_return.append(mix_strategy["Losers"][0])
        flp_return.append(mix_strategy["Winners - Losers"][0])
    
    
    for z in range(len(ww_return)):
        ww_return[z] = float(ww_return[z].split("%")[0]) / 100
        wl_return[z] = float(wl_return[z].split("%")[0]) / 100
        wp_return[z] = float(wp_return[z].split("%")[0]) / 100
        mw_return[z] = float(mw_return[z].split("%")[0]) / 100
        ml_return[z] = float(ml_return[z].split("%")[0]) / 100
        mp_return[z] = float(mp_return[z].split("%")[0]) / 100
        lw_return[z] = float(lw_return[z].split("%")[0]) / 100
        ll_return[z] = float(ll_return[z].split("%")[0]) / 100
        lp_return[z] = float(lp_return[z].split("%")[0]) / 100
        fww_return[z] = float(fww_return[z].split("%")[0]) / 100
        fwl_return[z] = float(fwl_return[z].split("%")[0]) / 100
        fwp_return[z] = float(fwp_return[z].split("%")[0]) / 100
        fmw_return[z] = float(fmw_return[z].split("%")[0]) / 100
        fml_return[z] = float(fml_return[z].split("%")[0]) / 100
        fmp_return[z] = float(fmp_return[z].split("%")[0]) / 100
        flw_return[z] = float(flw_return[z].split("%")[0]) / 100
        fll_return[z] = float(fll_return[z].split("%")[0]) / 100
        flp_return[z] = float(flp_return[z].split("%")[0]) / 100
    
    Mix_Strategy = pd.DataFrame(columns=["Portfolio Classified by FT", "Ave. Monthly Return", "Ave. Monthly Return Excluding Farvardin"],
                                index=["Winner", "Winner", "Winner", "Middle", "Middle", "Middle", "Loser", "Loser", "Loser"])
    Mix_Strategy.index.name = "Portfolio Classified by MG"
    Mix_Strategy.iloc[[0, 3, 6], 0] = "Winner"
    Mix_Strategy.iloc[[1, 4, 7], 0] = "Loser"
    Mix_Strategy.iloc[[2, 5, 8], 0] = "Winner - Loser"
    Mix_Strategy.iloc[0, 1] = str(round(np.mean(ww_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[1, 1] = str(round(np.mean(wl_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[2, 1] = str(round(np.mean(wp_return) * 100, 2)) + "% (" + str(round(np.mean(wp_return) * np.sqrt(len(wp_return)) / np.std(wp_return), 2)) + ")"
    Mix_Strategy.iloc[3, 1] = str(round(np.mean(mw_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[4, 1] = str(round(np.mean(ml_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[5, 1] = str(round(np.mean(mp_return) * 100, 2)) + "% (" + str(round(np.mean(mp_return) * np.sqrt(len(mp_return)) / np.std(mp_return), 2)) + ")"
    Mix_Strategy.iloc[6, 1] = str(round(np.mean(lw_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[7, 1] = str(round(np.mean(ll_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[8, 1] = str(round(np.mean(lp_return) * 100, 2)) + "% (" + str(round(np.mean(lp_return) * np.sqrt(len(lp_return)) / np.std(lp_return), 2)) + ")"
    Mix_Strategy.iloc[0, 2] = str(round(np.mean(fww_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[1, 2] = str(round(np.mean(fwl_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[2, 2] = str(round(np.mean(fwp_return) * 100, 2)) + "% (" + str(round(np.mean(fwp_return) * np.sqrt(len(fwp_return)) / np.std(fwp_return), 2)) + ")"
    Mix_Strategy.iloc[3, 2] = str(round(np.mean(fmw_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[4, 2] = str(round(np.mean(fml_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[5, 2] = str(round(np.mean(fmp_return) * 100, 2)) + "% (" + str(round(np.mean(fmp_return) * np.sqrt(len(fmp_return)) / np.std(fmp_return), 2)) + ")"
    Mix_Strategy.iloc[6, 2] = str(round(np.mean(flw_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[7, 2] = str(round(np.mean(fll_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[8, 2] = str(round(np.mean(flp_return) * 100, 2)) + "% (" + str(round(np.mean(flp_return) * np.sqrt(len(flp_return)) / np.std(flp_return), 2)) + ")"
    return Mix_Strategy

def FT_MG(df, ind_ret, stocks_ret, mc, J, K):
    """
    Implements a Mix-strategy for given financial data.

    Parameters:
    df (pandas DataFrame): The dataframe containing the original financial data.
    ind_ret (pandas DataFrame): The dataframe containing the return of the industry.
    stocks_ret (pandas DataFrame): The dataframe containing the return of stocks.
    mc (pandas DataFrame): The dataframe containing the market capitalization of stocks.
    J (int): The number of stocks to be considered as winners.
    K (int): The number of stocks to be considered as losers.

    Returns:
    A dictionary with the following keys:
        - "Winners" : The returns of the portfolio with winners strategy
        - "Losers" : The returns of the portfolio with losers strategy
        - "Winners - Losers" : The returns of the portfolio with winners minus losers strategy
    """
    df = df.iloc[:, 1:].copy(deep = True)
    df2 = d2m(year_high(df)).iloc[:-1, :]
    quantile7 = df2.quantile(0.7, axis = 1)
    quantile3 = df2.quantile(0.3, axis = 1)
    ww_return = []
    wl_return = []
    wp_return = []
    mw_return = []
    ml_return = []
    mp_return = []
    lw_return = []
    ll_return = []
    lp_return = []
    for i in range(100, 110):#len(df2)-5):
        winners, losers, middles = FT_Ranker(df2, mc, quantile7, quantile3, i)
        #FT Winners
        mix_strategy = MG_Strategy(ind_ret, stocks_ret[winners], J, K)
        ww_return.append(mix_strategy["Winners"][0])
        wl_return.append(mix_strategy["Losers"][0])
        wp_return.append(mix_strategy["Winners - Losers"][0])

        #FT Middles
        mix_strategy = MG_Strategy(ind_ret, stocks_ret[middles], J, K)
        mw_return.append(mix_strategy["Winners"][0])
        ml_return.append(mix_strategy["Losers"][0])
        mp_return.append(mix_strategy["Winners - Losers"][0])

        #FT Losers
        mix_strategy = MG_Strategy(ind_ret, stocks_ret[losers], J, K)
        lw_return.append(mix_strategy["Winners"][0])
        ll_return.append(mix_strategy["Losers"][0])
        lp_return.append(mix_strategy["Winners - Losers"][0])
    
    ind_ret2 = Farvardin(ind_ret)[1]
    stocks_ret2 = Farvardin(stocks_ret)[1]
    mc3 = Farvardin(mc)[1]
    df2 = Farvardin(df2)[1]
    quantile7 = df2.quantile(0.7, axis = 1)
    quantile3 = df2.quantile(0.3, axis = 1)
    fww_return = []
    fwl_return = []
    fwp_return = []
    fmw_return = []
    fml_return = []
    fmp_return = []
    flw_return = []
    fll_return = []
    flp_return = []
    for i in range(100, 110):#len(df2)-5):
        winners, losers, middles = FT_Ranker(df2, mc3, quantile7, quantile3, i)
        #FT Winners
        mix_strategy = MG_Strategy(ind_ret2, stocks_ret2[winners], J, K)
        fww_return.append(mix_strategy["Winners"][0])
        fwl_return.append(mix_strategy["Losers"][0])
        fwp_return.append(mix_strategy["Winners - Losers"][0])

        #FT Middles
        mix_strategy = MG_Strategy(ind_ret2, stocks_ret2[middles], J, K)
        fmw_return.append(mix_strategy["Winners"][0])
        fml_return.append(mix_strategy["Losers"][0])
        fmp_return.append(mix_strategy["Winners - Losers"][0])

        #FT Losers
        mix_strategy = MG_Strategy(ind_ret2, stocks_ret2[losers], J, K)
        flw_return.append(mix_strategy["Winners"][0])
        fll_return.append(mix_strategy["Losers"][0])
        flp_return.append(mix_strategy["Winners - Losers"][0])
    
    
    for z in range(len(ww_return)):
        ww_return[z] = float(ww_return[z].split("%")[0]) / 100
        wl_return[z] = float(wl_return[z].split("%")[0]) / 100
        wp_return[z] = float(wp_return[z].split("%")[0]) / 100
        mw_return[z] = float(mw_return[z].split("%")[0]) / 100
        ml_return[z] = float(ml_return[z].split("%")[0]) / 100
        mp_return[z] = float(mp_return[z].split("%")[0]) / 100
        lw_return[z] = float(lw_return[z].split("%")[0]) / 100
        ll_return[z] = float(ll_return[z].split("%")[0]) / 100
        lp_return[z] = float(lp_return[z].split("%")[0]) / 100
        fww_return[z] = float(fww_return[z].split("%")[0]) / 100
        fwl_return[z] = float(fwl_return[z].split("%")[0]) / 100
        fwp_return[z] = float(fwp_return[z].split("%")[0]) / 100
        fmw_return[z] = float(fmw_return[z].split("%")[0]) / 100
        fml_return[z] = float(fml_return[z].split("%")[0]) / 100
        fmp_return[z] = float(fmp_return[z].split("%")[0]) / 100
        flw_return[z] = float(flw_return[z].split("%")[0]) / 100
        fll_return[z] = float(fll_return[z].split("%")[0]) / 100
        flp_return[z] = float(flp_return[z].split("%")[0]) / 100
    
    Mix_Strategy = pd.DataFrame(columns=["Portfolio Classified by MG", "Ave. Monthly Return", "Ave. Monthly Return Excluding Farvardin"],
                                index=["Winner", "Winner", "Winner", "Middle", "Middle", "Middle", "Loser", "Loser", "Loser"])
    Mix_Strategy.index.name = "Portfolio Classified by FT"
    Mix_Strategy.iloc[[0, 3, 6], 0] = "Winner"
    Mix_Strategy.iloc[[1, 4, 7], 0] = "Loser"
    Mix_Strategy.iloc[[2, 5, 8], 0] = "Winner - Loser"
    Mix_Strategy.iloc[0, 1] = str(round(np.mean(ww_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[1, 1] = str(round(np.mean(wl_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[2, 1] = str(round(np.mean(wp_return) * 100, 2)) + "% (" + str(round(np.mean(wp_return) * np.sqrt(len(wp_return)) / np.std(wp_return), 2)) + ")"
    Mix_Strategy.iloc[3, 1] = str(round(np.mean(mw_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[4, 1] = str(round(np.mean(ml_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[5, 1] = str(round(np.mean(mp_return) * 100, 2)) + "% (" + str(round(np.mean(mp_return) * np.sqrt(len(mp_return)) / np.std(mp_return), 2)) + ")"
    Mix_Strategy.iloc[6, 1] = str(round(np.mean(lw_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[7, 1] = str(round(np.mean(ll_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[8, 1] = str(round(np.mean(lp_return) * 100, 2)) + "% (" + str(round(np.mean(lp_return) * np.sqrt(len(lp_return)) / np.std(lp_return), 2)) + ")"
    Mix_Strategy.iloc[0, 2] = str(round(np.mean(fww_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[1, 2] = str(round(np.mean(fwl_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[2, 2] = str(round(np.mean(fwp_return) * 100, 2)) + "% (" + str(round(np.mean(fwp_return) * np.sqrt(len(fwp_return)) / np.std(fwp_return), 2)) + ")"
    Mix_Strategy.iloc[3, 2] = str(round(np.mean(fmw_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[4, 2] = str(round(np.mean(fml_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[5, 2] = str(round(np.mean(fmp_return) * 100, 2)) + "% (" + str(round(np.mean(fmp_return) * np.sqrt(len(fmp_return)) / np.std(fmp_return), 2)) + ")"
    Mix_Strategy.iloc[6, 2] = str(round(np.mean(flw_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[7, 2] = str(round(np.mean(fll_return) * 100, 2)) + "%"
    Mix_Strategy.iloc[8, 2] = str(round(np.mean(flp_return) * 100, 2)) + "% (" + str(round(np.mean(flp_return) * np.sqrt(len(flp_return)) / np.std(flp_return), 2)) + ")"
    return Mix_Strategy

def Ranker(ticker, df, ind_ret, ret, mc, J, t, labels, t2 = False):
    """
    Creates binary labels for a given stock based on 3 different ranking methods.
    
    Parameters:
    ticker (str): Ticker symbol of the stock to be labeled.
    df (pandas.DataFrame): DataFrame containing stock information.
    ind_ret (pandas.DataFrame): DataFrame containing industry return information.
    ret (pandas.DataFrame): DataFrame containing return information of stocks.
    mc (pandas.DataFrame): DataFrame containing market capitalization information of stocks.
    J (int): Number of time periods to use for labeling.
    t (int): Index in the DataFrames to start labeling from.
    labels (list of str): Names of the binary labels to be created.
    t2 (bool, optional): Indicates whether to return labels only for t2 (default is False).
    
    Returns:
    pandas.DataFrame: A DataFrame with the binary labels for the stock.
    """
    df = df.iloc[:, 1:].copy(deep = True)
    df2 = d2m(year_high(df)).iloc[:-1, :]
    quantile7 = df2.quantile(0.7, axis = 1)
    quantile3 = df2.quantile(0.3, axis = 1)
    Labels = pd.DataFrame(columns=labels, index=[ret.index[t]])
    Labels.iloc[0, :] = 0
    for j in range(2, J+2):
        J_winners, J_losers, J_middles = JK_Ranker(ret, mc, J, t-j)
        M_winners, M_losers, M_middles = MG_Ranker(ind_ret, ret, J, t-j)
        FH_winners, FH_losers, FH_middles = FT_Ranker(df2, mc, quantile7, quantile3, t-j)
        if ticker in J_winners:
            Labels.loc[:, "JH"+str(j)] = 1
        elif ticker in J_losers:
            Labels.loc[:, "JL"+str(j)] = 1
        if ticker in M_winners:
            Labels.loc[:, "MH"+str(j)] = 1
        elif ticker in M_losers:
            Labels.loc[:, "ML"+str(j)] = 1
        if ticker in FH_winners:
            Labels.loc[:, "FHH"+str(j)] = 1
        elif ticker in FH_losers:
            Labels.loc[:, "FHL"+str(j)] = 1
    return Labels

def Fama_MacBeth(df, ind_ret, ret, mc, J, t2 = False):
    """
The function Fama_MacBeth performs the Fama-MacBeth two-pass cross-sectional regression on a given dataframe df with industry returns ind_ret, asset returns ret, market capitalization mc, and number of quantiles J.

The first step of the Fama-MacBeth regression involves running time-series regressions for each asset, regressing the excess return of the asset against size and past return, and other interaction terms formed from the rankings of the assets within quantiles based on size and past returns. The regression coefficients are stored in the dataframe coefs.

The second step involves cross-sectional regressions at each time step, regressing the excess returns of all assets on their estimated coefficients from the time-series regressions. The regression coefficients for the interaction terms at each time step are stored in the dataframe premiums.

The final result is a dataframe Results with the average coefficient and t-statistic for each interaction term, either with or without the first month ("Farvardin") included based on the argument t2.

Parameters:
df (DataFrame): The dataframe of all the data.
ind_ret (Series): The series of industry returns.
ret (DataFrame): The dataframe of asset returns.
mc (DataFrame): The dataframe of market capitalization of each asset.
J (int): The number of quantiles to split the assets into.
t2 (bool): Whether or not to exclude the first month ("Farvardin"). Default is False.

Returns:
DataFrame: A dataframe of average coefficients and t-statistics for each interaction term.
"""
    labels = []
    for i in ["JH", "JL", "MH", "ML", "FHH", "FHL"]:
        for j in range(2, J+2):
            labels.append(i+str(j))
    coefs = pd.DataFrame(index = ret.columns, columns = ["size, R_t-1"] + labels + ["Intercept"])

    #TimeSeries Regressions
    for ticker in ret.columns:
        mc2 = mc[ticker].dropna().to_frame().iloc[:-1, :]
        ret2 = ret[ticker].dropna().to_frame().loc[mc2.index, :]
        ret_t = ret2.iloc[1:, :]
        ret_t1 = ret2.iloc[:-1, :]
        mc_t1 = mc2.iloc[:-1, :]
        Xs = mc_t1.merge(ret_t1, how="left", on="Date")
        Xs.rename(columns = {Xs.columns[0]:"size", Xs.columns[1]:"R_t-1"}, inplace = True)
        t = ret.index.get_indexer([mc2.index[0]])[0]
        Labels = Ranker(ticker, df, ind_ret, ret, mc, J, t)
        for i in range(1, len(mc2.index)-1):
            t = ret.index.get_indexer([mc2.index[i]])[0]
            Labels = Labels.append(Ranker(ticker, df, ind_ret, ret, mc, J, t, labels))
        Labels.index.name = "Date"
        Xs = Xs.merge(Labels, how="left", on="Date")
        Xs["Intercept"] = 1
        Xs.index = range(len(Xs))
        ret_t.index = range(len(ret_t))
        fitted_model = sm.OLS(ret_t.astype(float), Xs.astype(float), missing="drop").fit()
        coefs.loc[ticker, :] = fitted_model.params
    
    #Cross-Sectional Regressions
    labels = ["size, R_t-1"] + labels + ["Intercept"]
    premiums = pd.DataFrame(index = ret.index, columns = labels)
    for t in ret.index:
        ret2 = ret.loc[t, :].dropna(axis = 1)
        tickers = ret2.columns
        coef = coefs.loc[tickers, :]
        ret2.index = range(len(ret2))
        coef.index = range(len(coef))
        fitted_model = sm.OLS(ret2, coef, missing="drop").fit
        premiums.loc[t, :] = fitted_model.params

    if t2:
        colname = "Farvardin Excluded"
    else:
        colname = "Farvardin Included"
    Results = pd.DataFrame(index = labels, columns = [colname])
    for i in Results.index:
        Results.loc[i, :] = str(premiums.loc[:, i].mean()) + " (" + str(premiums.loc[:, i].mean() * np.sqrt(len(premiums)) / premiums.loc[:, i].std()) + ")"

    return Results

def Fama_MacBeth_lag(df, ind_ret, ret, mc, J, lag, t2 = False):
    """
Fama_MacBeth_lag is a function that performs Fama-MacBeth regression with a specified lag period to estimate the cross-sectional average return premiums of a set of assets.

Inputs:
df (DataFrame): DataFrame containing stock information for all assets
ind_ret (DataFrame): DataFrame containing the return of an index for all assets
ret (DataFrame): DataFrame containing the return of all assets
mc (DataFrame): DataFrame containing the market capitalization of all assets
J (int): Number of portfolios to be formed
lag (int): The specified lag period
t2 (Boolean, optional): If t2=True, exclude the month of Farvardin from the analysis, default is False

Returns:
Results (DataFrame): DataFrame containing the estimated average return premiums for each factor and the intercept, with the mean and t-statistics in the format of "mean (t-stat)".
"""
    labels = []
    for i in ["JH", "JL", "MH", "ML", "FHH", "FHL"]:
        for j in range(2, J+2):
            labels.append(i+str(j))
    coefs = pd.DataFrame(index = ret.columns, columns = ["size, R_t-1"] + labels + ["Intercept"])

    #TimeSeries Regressions
    for ticker in ret.columns:
        mc2 = mc[ticker].dropna().to_frame().iloc[:-1, :]
        ret2 = ret[ticker].dropna().to_frame().loc[mc2.index, :]
        ret_t = ret2.iloc[1:, :]
        ret_t1 = ret2.iloc[:-1, :]
        mc_t1 = mc2.iloc[:-1, :]
        Xs = mc_t1.merge(ret_t1, how="left", on="Date")
        Xs.rename(columns = {Xs.columns[0]:"size", Xs.columns[1]:"R_t-1"}, inplace = True)
        t = ret.index.get_indexer([mc2.index[0]])[0]
        Labels = Ranker(ticker, df, ind_ret, ret, mc, J, t)
        for i in range(1, len(mc2.index)-1):
            t = ret.index.get_indexer([mc2.index[i]])[0] - lag #Effect of lag
            Labels = Labels.append(Ranker(ticker, df, ind_ret, ret, mc, J, t, labels))
        Labels.index.name = "Date"
        Xs = Xs.merge(Labels, how="left", on="Date")
        Xs["Intercept"] = 1
        Xs.index = range(len(Xs))
        ret_t.index = range(len(ret_t))
        fitted_model = sm.OLS(ret_t.astype(float), Xs.astype(float), missing="drop").fit()
        coefs.loc[ticker, :] = fitted_model.params
    
    #Cross-Sectional Regressions
    labels = ["size, R_t-1"] + labels + ["Intercept"]
    premiums = pd.DataFrame(index = ret.index, columns = labels)
    for t in ret.index:
        ret2 = ret.loc[t, :].dropna(axis = 1)
        tickers = ret2.columns
        coef = coefs.loc[tickers, :]
        ret2.index = range(len(ret2))
        coef.index = range(len(coef))
        fitted_model = sm.OLS(ret2, coef, missing="drop").fit
        premiums.loc[t, :] = fitted_model.params

    if t2:
        colname = "Farvardin Excluded"
    else:
        colname = "Farvardin Included"
    Results = pd.DataFrame(index = labels, columns = [colname])
    for i in Results.index:
        Results.loc[i, :] = str(premiums.loc[:, i].mean()) + " (" + str(premiums.loc[:, i].mean() * np.sqrt(len(premiums)) / premiums.loc[:, i].std()) + ")"

    return Results

def Ranker_low(ticker, df, ind_ret, ret, mc, J, t, labels, t2 = False):
    """
    This function calculates the ranking labels for a given stock (`ticker`) based on the stock's performance
    compared to other stocks in the dataframe (`df`). The ranking is done based on the stock's year-low, as well as
    its performance in comparison to the industry (`ind_ret`) and the market (`ret`). The ranking is performed over 
    a certain period of time `t-j` (specified by `J`) and the resulting labels are stored in a DataFrame (`Labels`).

    Parameters:
    ticker (str): The name of the stock for which to calculate the ranking labels.
    df (pd.DataFrame): A DataFrame containing the stock prices for the stocks in the market.
    ind_ret (pd.DataFrame): A DataFrame containing the returns for the industry for the given stocks.
    ret (pd.DataFrame): A DataFrame containing the returns for the market for the given stocks.
    mc (pd.DataFrame): A DataFrame containing the market capitalization values for the given stocks.
    J (int): The number of periods to consider for the ranking.
    t (int): The current time step.
    labels (list): A list of strings that specify the names of the columns to use for the resulting labels.
    t2 (bool, optional): A flag indicating whether to perform an additional calculation (default is False).

    Returns:
    pd.DataFrame: A DataFrame containing the resulting labels for the given stock.
    """
    df = df.iloc[:, 1:].copy(deep = True)
    df2 = d2m(year_high(df)).iloc[:-1, :]
    quantile7 = df2.quantile(0.7, axis = 1)
    quantile3 = df2.quantile(0.3, axis = 1)
    Labels = pd.DataFrame(columns=labels, index=[ret.index[t]])
    Labels.iloc[0, :] = 0
    for j in range(2, J+2):
        J_winners, J_losers, J_middles = JK_Ranker(ret, mc, J, t-j)
        M_winners, M_losers, M_middles = MG_Ranker(ind_ret, ret, J, t-j)
        FH_winners, FH_losers, FH_middles = FT_Ranker(df2, mc, quantile3, quantile7, t-j) # By changing the place of q7 & q3, the FT_Ranker will rank based on 52 week low.
        if ticker in J_winners:
            Labels.loc[:, "JH"+str(j)] = 1
        elif ticker in J_losers:
            Labels.loc[:, "JL"+str(j)] = 1
        if ticker in M_winners:
            Labels.loc[:, "MH"+str(j)] = 1
        elif ticker in M_losers:
            Labels.loc[:, "ML"+str(j)] = 1
        if ticker in FH_winners:
            Labels.loc[:, "FHH"+str(j)] = 1
        elif ticker in FH_losers:
            Labels.loc[:, "FHL"+str(j)] = 1
    return Labels

def Fama_MacBeth_low(df, ind_ret, ret, mc, J, t2 = False):
    """
    This function performs a Fama-MacBeth regression analysis to calculate the premiums for a set of stocks in the
    market. The analysis is performed based on the stocks' performance compared to the industry and the market, as well 
    as their year-low values. The function first calculates the premiums for each stock using a time-series regression, 
    and then aggregates the results using a cross-sectional regression. The resulting premiums are stored in a DataFrame 
    (`premiums`).

    Parameters:
    df (pd.DataFrame): A DataFrame containing the stock prices for the stocks in the market.
    ind_ret (pd.DataFrame): A DataFrame containing the returns for the industry for the given stocks.
    ret (pd.DataFrame): A DataFrame containing the returns for the market for the given stocks.
    mc (pd.DataFrame): A DataFrame containing the market capitalization values for the given stocks.
    J (int): The number of periods to consider for the ranking.
    t2 (bool, optional): A flag indicating whether to perform an additional calculation (default is False).

    Returns:
    pd.DataFrame: A DataFrame containing the resulting premiums for each stock in the market.
    """
    labels = []
    for i in ["JH", "JL", "MH", "ML", "FHH", "FHL"]:
        for j in range(2, J+2):
            labels.append(i+str(j))
    coefs = pd.DataFrame(index = ret.columns, columns = ["size, R_t-1"] + labels + ["Intercept"])

    #TimeSeries Regressions
    for ticker in ret.columns:
        mc2 = mc[ticker].dropna().to_frame().iloc[:-1, :]
        ret2 = ret[ticker].dropna().to_frame().loc[mc2.index, :]
        ret_t = ret2.iloc[1:, :]
        ret_t1 = ret2.iloc[:-1, :]
        mc_t1 = mc2.iloc[:-1, :]
        Xs = mc_t1.merge(ret_t1, how="left", on="Date")
        Xs.rename(columns = {Xs.columns[0]:"size", Xs.columns[1]:"R_t-1"}, inplace = True)
        t = ret.index.get_indexer([mc2.index[0]])[0]
        Labels = Ranker_low(ticker, df, ind_ret, ret, mc, J, t)
        for i in range(1, len(mc2.index)-1):
            t = ret.index.get_indexer([mc2.index[i]])[0]
            Labels = Labels.append(Ranker_low(ticker, df, ind_ret, ret, mc, J, t, labels))
        Labels.index.name = "Date"
        Xs = Xs.merge(Labels, how="left", on="Date")
        Xs["Intercept"] = 1
        Xs.index = range(len(Xs))
        ret_t.index = range(len(ret_t))
        fitted_model = sm.OLS(ret_t.astype(float), Xs.astype(float), missing="drop").fit()
        coefs.loc[ticker, :] = fitted_model.params
    
    #Cross-Sectional Regressions
    labels = ["size, R_t-1"] + labels + ["Intercept"]
    premiums = pd.DataFrame(index = ret.index, columns = labels)
    for t in ret.index:
        ret2 = ret.loc[t, :].dropna(axis = 1)
        tickers = ret2.columns
        coef = coefs.loc[tickers, :]
        ret2.index = range(len(ret2))
        coef.index = range(len(coef))
        fitted_model = sm.OLS(ret2, coef, missing="drop").fit
        premiums.loc[t, :] = fitted_model.params

    if t2:
        colname = "Farvardin Excluded"
    else:
        colname = "Farvardin Included"
    Results = pd.DataFrame(index = labels, columns = [colname])
    for i in Results.index:
        Results.loc[i, :] = str(premiums.loc[:, i].mean()) + " (" + str(premiums.loc[:, i].mean() * np.sqrt(len(premiums)) / premiums.loc[:, i].std()) + ")"

    return Results

