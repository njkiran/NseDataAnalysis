# -*- coding: utf-8 -*-
"""
Created on Mon Aug 10 23:01:14 2020

@author: NAGUMN
"""
import datetime 
import backtrader as bt
import backtrader.feeds as btfeeds

data = btfeeds.GenericCSVData(
    dataname='NATGAS_FUT.txt',

    fromdate=datetime.datetime(2020, 6, 26),
    todate=datetime.datetime(2020, 8, 10),

    nullvalue=0.0,

    dtformat=('%d/%m/%y'),

    datetime=0,
    high=1,
    low=2,
    open=3,
    close=4,
    volume=5,
    openinterest=-1
    
    )

class Strat2_BGTMA_SLSMA(bt.Strategy):
    
    params = (
        ('maperiod',15), # Tuple of tuples containing any variable settings required by the strategy.
        ('printlog',False), # Stop printing the log of the trading strategy
        
    )
    
    def __init__(self):
        self.dataclose= self.datas[0].close    # Keep a reference to the "close" line in the data[0] dataseries
        self.order = None # Property to keep track of pending orders.  There are no orders when the strategy is initialized.
        self.buyprice = None
        self.buycomm = None
        
        # Add SimpleMovingAverage indicator for use in the trading strategy
        self.sma = bt.indicators.SimpleMovingAverage( 
            self.datas[0], period=self.params.maperiod)
    
    def log(self, txt, dt=None, doprint=False):
        if self.params.printlog or doprint: # Add if statement to only log of printlog or doprint is True
            dt = dt or self.datas[0].datetime.date(0)
            print('{0},{1}'.format(dt.isoformat(),txt))
    
    def notify_order(self, order):
        # 1. If order is submitted/accepted, do nothing 
        if order.status in [order.Submitted, order.Accepted]:
            return
        # 2. If order is buy/sell executed, report price executed
        if order.status in [order.Completed]: 
            if order.isbuy():
                self.log('BUY EXECUTED, Price: {0:8.2f}, Size: {1:8.2f} Cost: {2:8.2f}, Comm: {3:8.2f}'.format(
                    order.executed.price,
                    order.executed.size,
                    order.executed.value,
                    order.executed.comm))
                
                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:
                self.log('SELL EXECUTED, {0:8.2f}, Size: {1:8.2f} Cost: {2:8.2f}, Comm{3:8.2f}'.format(
                    order.executed.price, 
                    order.executed.size, 
                    order.executed.value,
                    order.executed.comm))
            
            self.bar_executed = len(self) #when was trade executed
        # 3. If order is canceled/margin/rejected, report order canceled
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')
            
        self.order = None
    
    def notify_trade(self,trade):
        if not trade.isclosed:
            return
        
        self.log('OPERATION PROFIT, GROSS {0:8.2f}, NET {1:8.2f}'.format(
            trade.pnl, trade.pnlcomm))
    
    def next(self):
        # Log the closing prices of the series from the reference
        self.log('Close, {0:8.2f}'.format(self.dataclose[0]))

        if self.order: # check if order is pending, if so, then break out
            return
                
        # since there is no order pending, are we in the market?    
        if not self.position: # not in the market
            if self.dataclose[0] > self.sma[0]:
                self.log('BUY CREATE {0:8.2f}'.format(self.dataclose[0]))
                self.order = self.buy()           
        else: # in the market
            if self.dataclose[0] < self.sma[0]:
                self.log('SELL CREATE, {0:8.2f}'.format(self.dataclose[0]))
                self.order = self.sell()
                
    def stop(self):
        self.log('MA Period: {0:8.2f} Ending Value: {1:8.2f}'.format(
            self.params.maperiod, 
            self.broker.getvalue()),
                 doprint=True)
        
cerebro = bt.Cerebro()  
cerebro.adddata(data) 
strats = cerebro.optstrategy(
    Strat2_BGTMA_SLSMA,
    maperiod=range(10,31),
    printlog=False)
cerebro.addsizer(bt.sizers.FixedSize,stake=10)
cerebro.broker.setcash(1000.0) 
cerebro.broker.setcommission(commission=0.0) 

