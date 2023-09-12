# Lesson1：Backtrader来啦
# link: https://mp.weixin.qq.com/s/7S4AnbUfQy2kCZhuFN1dZw

# %%
import backtrader as bt
import pandas as pd
import datetime

import tushare as ts
import json

# 从 Tushare 获取数据
with open(r'Data/tushare_token.json', 'r') as load_json:
    token_json = json.load(load_json)
token = token_json['token']
ts.set_token(token)
pro = ts.pro_api(token)


def get_data_by_tushare(code, start_date, end_date):
    df = ts.pro_bar(ts_code=code, adj='qfq', start_date=start_date, end_date=end_date)
    df = df[['trade_date', 'open', 'high', 'low', 'close', 'vol']]
    df.columns = ['trade_date', 'open', 'high', 'low', 'close', 'volume']
    df.trade_date = pd.to_datetime(df.trade_date)
    df.index = df.trade_date
    df.sort_index(inplace=True)
    df.fillna(0.0, inplace=True)

    return df


cerebro = bt.Cerebro()

st_date = datetime.datetime(2019, 1, 1)
ed_date = datetime.datetime(2021, 10, 15)

trade_info = pd.read_csv("Data/trade_info.csv", parse_dates=['trade_date'])

# for sec_code in trade_info['sec_code'].unique():
#     data_tushare = get_data_by_tushare(sec_code, '20190101', '20211016')
#     data_tushare.to_csv("Data/{}_{}_{}.csv".format(sec_code, '20190101', '20211016'), index=False)
#     print(f"{sec_code} Downloaded !")


for sec_code in trade_info['sec_code'].unique():
    # 以 GenericCSVData 为例进行参数说明（其他导入函数参数类似）
    data_feed = bt.feeds.GenericCSVData(dataname="Data/{}_{}_{}.csv".format(sec_code, '20190101', '20211016'),
                                        # 数据源，CSV文件名 或 Dataframe对象
                                        fromdate=st_date,  # 读取的起始时间
                                        todate=ed_date,  # 读取的结束时间
                                        nullvalue=0.0,  # 缺失值填充
                                        dtformat=('%Y-%m-%d'),  # 日期解析的格式
                                        # 下面是数据表格默认包含的 7 个指标，取值对应指标在 daily_price.csv 中的列索引位置
                                        datetime=0,  # 告诉 GenericCSVData， datetime 在 daily_price.csv 文件的第1列
                                        high=2,
                                        low=3,
                                        open=1,
                                        close=4,
                                        volume=5,
                                        openinterest=-1)  # 如果取值为 -1 , 告诉 GenericCSVData 该指标不存在
    cerebro.adddata(data_feed, name=sec_code)
    print(f"{sec_code} Done !")

print("All stock Done !")



# 恒瑞医药
# data_000006_SZ = get_data_by_tushare('000006.SZ', '20190101', '20211016')
# data_000008_SZ = get_data_by_tushare('000008.SZ', '20190101', '20211016')
# 贵州茅台
# data2 = get_data_bytushare('600519.SH', '20200101', '20211016')
# 海天味业
# data3 = get_data_bytushare('603288.SH', '20200101', '20211016')

# data1.to_csv("Data/000006.SZ.csv", index=False)
# data2.to_csv("Data/600519.SH.csv", index=False)
# data3.to_csv("Data/603288.SH.csv", index=False)

# 实例化 cerebro

# daily_price = pd.read_csv("Data/000006.SZ.csv", parse_dates=['trade_date'])
#
#
# datafeed_000008_SZ = bt.feeds.PandasData(dataname=data_000008_SZ, fromdate=st_date, todate=ed_date)
# cerebro.adddata(datafeed_000008_SZ, name='000008.SZ')


#
# # 添加 600519.SH 的行情数据
# datafeed2 = bt.feeds.PandasData(dataname=data2, fromdate=st_date, todate=ed_date)
# cerebro.adddata(datafeed2, name='600519.SH')
#
# # 添加 603288.SH 的行情数据
# datafeed3 = bt.feeds.PandasData(dataname=data3, fromdate=st_date, todate=ed_date)
# cerebro.adddata(datafeed3, name='603288.SH')


# %%

# 按股票代码，依次循环传入数据
# for stock in daily_price['sec_code'].unique():
#     # 日期对齐
#     data = pd.DataFrame(daily_price['datetime'].unique(), columns=['datetime'])  # 获取回测区间内所有交易日
#     df = daily_price.query(f"sec_code=='{stock}'")[
#         ['datetime', 'open', 'high', 'low', 'close', 'volume', 'openinterest']]
#     data_ = pd.merge(data, df, how='left', on='datetime')
#     data_ = data_.set_index("datetime")
#     # print(data_.dtypes)
#     # 缺失值处理：日期对齐时会使得有些交易日的数据为空，所以需要对缺失数据进行填充
#     data_.loc[:, ['volume', 'openinterest']] = data_.loc[:, ['volume', 'openinterest']].fillna(0)
#     # data_.loc[:, ['open', 'high', 'low', 'close']] = data_.loc[:, ['open', 'high', 'low', 'close']].fillna(method='pad')
#     data_.loc[:, ['open', 'high', 'low', 'close']] = data_.loc[:, ['open', 'high', 'low', 'close']].fillna(method='pad')
#     data_.loc[:, ['open', 'high', 'low', 'close']] = data_.loc[:, ['open', 'high', 'low', 'close']].fillna(0)
#     # 导入数据
#     datafeed = bt.feeds.PandasData(dataname=data_, fromdate=datetime.datetime(2019, 1, 2),
#                                    todate=datetime.datetime(2021, 1, 28))
#     cerebro.adddata(datafeed, name=stock)  # 通过 name 实现数据集与股票的一一对应
#     print(f"{stock} Done !")
#
# print("All stock Done !")


# %%

# 回测策略
class TestStrategy2(bt.Strategy):
    '''选股策略'''
    params = (('maperiod', 15),
              ('printlog', True),)

    def __init__(self):
        self.buy_stock = trade_info  # 保留调仓列表
        self.count = 0  # 用于计算 next 的循环次数
        # 读取调仓日期，即每月的最后一个交易日，回测时，会在这一天下单，然后在下一个交易日，以开盘价买入
        self.trade_dates = pd.to_datetime(self.buy_stock['trade_date'].unique()).tolist()
        self.order_list = []  # 记录以往订单，方便调仓日对未完成订单做处理
        self.buy_stocks_pre = []  # 记录上一期持仓

        # 打印数据集和数据集对应的名称
        print("------------- init 中的索引位置-------------")
        # 对 datetime 线进行索引时，xxx.date(X) 可以直接以“xxxx-xx-xx xx:xx:xx”的形式返回，X 就是索引位置，可以看做是传统 [X] 索引方式的改进版
        print("0 索引：", 'datetime', self.data0.lines.datetime.date(0), 'close', self.data0.lines.close[0])
        print("-1 索引：", 'datetime', self.data0.lines.datetime.date(-1), 'close', self.data0.lines.close[-1])
        print("-2 索引", 'datetime', self.data0.lines.datetime.date(-2), 'close', self.data0.lines.close[-2])
        print("1 索引：", 'datetime', self.data0.lines.datetime.date(1), 'close', self.data0.lines.close[1])
        print("2 索引", 'datetime', self.data0.lines.datetime.date(2), 'close', self.data0.lines.close[2])
        # 通过 get() 切片时，如果是从 ago=0 开始取，不会返回数据，从其他索引位置开始取，能返回数据
        print("从 0 开始往前取3天的收盘价：", self.data0.lines.close.get(ago=0, size=3))
        print("从-1开始往前取3天的收盘价：", self.data0.lines.close.get(ago=-1, size=3))
        print("从-2开始往前取3天的收盘价：", self.data0.lines.close.get(ago=-2, size=3))
        print("line的总长度：", self.data0.buflen())

    def next(self):

        # if (self.count == 289):
        #     print("aaa ", len(self.data0.lines.datetime))
        #
        # print(f"------------- next 的第{self.count + 1}次循环 --------------")
        # print("当前时点（今日）：", 'datetime', self.data0.lines.datetime.date(0), 'close', self.data0.lines.close[0])
        # print("往前推1天（昨日）：", 'datetime', self.data0.lines.datetime.date(-1), 'close', self.data0.lines.close[-1])
        # print("往前推2天（前日）", 'datetime', self.data0.lines.datetime.date(-2), 'close', self.data0.lines.close[-2])
        # print("前日、昨日、今日的收盘价：", self.data0.lines.close.get(ago=0, size=3))
        #
        # if ((self.count + 2) < len(self.data0.lines.datetime)):
        #     print("往后推1天（明日）：", 'datetime', self.data0.lines.datetime.date(1), 'close', self.data0.lines.close[1])
        #
        # if ((self.count + 1) < len(self.data0.lines.datetime)):
        #     print("往后推2天（明后日）", 'datetime', self.data0.lines.datetime.date(2), 'close',
        #           self.data0.lines.close[2])
        #
        # # 在 next() 中调用 len(self.data0)，返回的是当前已处理（已回测）的数据长度，会随着回测的推进动态增长
        # print("已处理的数据点：", len(self.data0))
        # # buflen() 返回整条线的总长度，固定不变；
        # print("line的总长度：", self.data0.buflen())
        # self.count += 1

        dt = self.data0.datetime.date(0)  # 获取当前的回测时间点
        # 如果是调仓日，则进行调仓操作

        trade_dates_ = []
        for a_date in self.trade_dates:
            trade_dates_.append(datetime.date(a_date.year, a_date.month, a_date.day))

        if dt in trade_dates_:
            # if dt in self.trade_dates:
            print("--------------{} 为调仓日----------".format(dt))
            # 在调仓之前，取消之前所下的没成交也未到期的订单
            if len(self.order_list) > 0:
                for od in self.order_list:
                    self.cancel(od)  # 如果订单未完成，则撤销订单
                self.order_list = []  # 重置订单列表
            # 提取当前调仓日的持仓列表
            buy_stocks_data = self.buy_stock.query(f"trade_date=='{dt}'")
            long_list = buy_stocks_data['sec_code'].tolist()
            print('long_list', long_list)  # 打印持仓列表
            # 对现有持仓中，调仓后不再继续持有的股票进行卖出平仓
            sell_stock = [i for i in self.buy_stocks_pre if i not in long_list]
            print('sell_stock', sell_stock)  # 打印平仓列表
            if len(sell_stock) > 0:
                print("-----------对不再持有的股票进行平仓--------------")
                for stock in sell_stock:
                    data = self.getdatabyname(stock)
                    if self.getposition(data).size > 0:
                        od = self.close(data=data)
                        self.order_list.append(od)  # 记录卖出订单
            # 买入此次调仓的股票：多退少补原则
            print("-----------买入此次调仓期的股票--------------")
            for stock in long_list:
                w = buy_stocks_data.query(f"sec_code=='{stock}'")['weight'].iloc[0]  # 提取持仓权重
                data = self.getdatabyname(stock)
                order = self.order_target_percent(data=data, target=w * 0.95)  # 为减少可用资金不足的情况，留 5% 的现金做备用
                self.order_list.append(order)

            self.buy_stocks_pre = long_list  # 保存此次调仓的股票列表

        # 交易记录日志（可省略，默认不输出结果）

    def log(self, txt, dt=None, doprint=False):
        if self.params.printlog or doprint:
            dt = dt or self.datas[0].datetime.date(0)
            print(f'{dt.isoformat()},{txt}')

    def notify_order(self, order):
        # 未被处理的订单
        if order.status in [order.Submitted, order.Accepted]:
            return
        # 已经处理的订单
        if order.status in [order.Completed, order.Canceled, order.Margin]:
            if order.isbuy():
                self.log(
                    'BUY EXECUTED, ref:%.0f，Price: %.2f, Cost: %.2f, Comm %.2f, Size: %.2f, Stock: %s' %
                    (order.ref,  # 订单编号
                     order.executed.price,  # 成交价
                     order.executed.value,  # 成交额
                     order.executed.comm,  # 佣金
                     order.executed.size,  # 成交量
                     order.data._name))  # 股票名称
            else:  # Sell
                self.log('SELL EXECUTED, ref:%.0f, Price: %.2f, Cost: %.2f, Comm %.2f, Size: %.2f, Stock: %s' %
                         (order.ref,
                          order.executed.price,
                          order.executed.value,
                          order.executed.comm,
                          order.executed.size,
                          order.data._name))


# 初始资金 100,000,000
cerebro.broker.setcash(100000000.0)
# 佣金，双边各 0.0003
cerebro.broker.setcommission(commission=0.0003)
# 滑点：双边各 0.0001
cerebro.broker.set_slippage_perc(perc=0.005)

cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='pnl')  # 返回收益率时序数据
cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='_AnnualReturn')  # 年化收益率
cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='_SharpeRatio')  # 夏普比率
cerebro.addanalyzer(bt.analyzers.DrawDown, _name='_DrawDown')  # 回撤

# 将编写的策略添加给大脑，别忘了 ！
cerebro.addstrategy(TestStrategy2)

# 启动回测
result = cerebro.run()
# 从返回的 result 中提取回测结果
start = result[0]
# 返回日度收益率序列
daily_return = pd.Series(start.analyzers.pnl.get_analysis())
# 打印评价指标
print("--------------- AnnualReturn -----------------")
print(start.analyzers._AnnualReturn.get_analysis())
print("--------------- SharpeRatio -----------------")
print(start.analyzers._SharpeRatio.get_analysis())
print("--------------- DrawDown -----------------")
print(start.analyzers._DrawDown.get_analysis())
