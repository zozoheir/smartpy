import numpy as np


class OrderBookReconstructor:

    def __init__(self, orderbook_depth_perc):

        self.mid = np.float('nan')
        self.tob_spread_absolute = np.float('nan')
        self.tob_spread_bps = np.float('nan')
        self.total_bid_size = np.float('nan')
        self.total_ask_size = np.float('nan')
        self.bid_price = np.float('nan')
        self.ask_price = np.float('nan')
        self.orderbook_depth_perc = orderbook_depth_perc

    def update(self, asks_dict, bids_dict):

        self.asks_dict = asks_dict
        self.bids_dict = bids_dict
        self._convertDictToNumpy()
        self.computeMid()
        self.computeSpreadAbs()
        self.computeSpreadBps()

    def _convertDictToNumpy(self):

        self.bid_prices = np.array([])
        self.bid_sizes = np.array([])
        self.ask_prices = np.array([])
        self.ask_sizes = np.array([])
        mid = (self.asks_dict[f"ask_price_{0}"]+self.bids_dict[f"bid_price_{0}"])/2

        for i in range(int(len(self.asks_dict.keys()) / 2)):
            self.ask_prices = np.append(self.ask_prices, self.asks_dict[f"ask_price_{i}"])
            self.ask_sizes = np.append(self.ask_sizes, self.asks_dict[f"ask_amount_{i}"])
            if self.asks_dict[f"ask_price_{i}"] > mid*(1+self.orderbook_depth_perc/100):
                #print(f"Stopped at {i} ask levels")
                break
        for i in range(int(len(self.bids_dict.keys()) / 2)):
            self.bid_prices = np.append(self.bid_prices, self.bids_dict[f"bid_price_{i}"])
            self.bid_sizes = np.append(self.bid_sizes, self.bids_dict[f"bid_amount_{i}"])
            if self.bids_dict[f"bid_price_{i}"] < mid*(1-self.orderbook_depth_perc/100):
                #print(f"Stopped at {i} bid levels")
                break

        # total bid & ask sizes
        self.total_bid_size = np.sum(self.bid_sizes)
        self.total_ask_size = np.sum(self.ask_sizes)
        self.bid_price = self.bid_prices[0]
        self.ask_price = self.ask_prices[0]


    def computeMid(self):
        # We only compute if it doesn't already exist
        if (not np.isnan(self.bid_price)) and (not np.isnan(self.ask_price)):
            self.mid = 0.5 * (self.bid_price + self.ask_price)
        else:
            self.mid = np.float('nan')

    def computeSpreadAbs(self):
        if (self.bid_prices[0] != 0.0) and (self.ask_prices[0] != 0.0):
            self.tob_spread_absolute = self.ask_prices[0] - self.bid_prices[0]
        else:
            self.tob_spread_absolute = np.float('nan')

    def computeSpreadBps(self):
        self.computeSpreadAbs()
        if np.isnan(self.mid):
            self.tob_spread_bps = np.float('nan')
        else:
            self.tob_spread_bps = self.tob_spread_absolute / self.mid * np.float(10000)

    def getVWSpreadAbs(self, size_usd):
        size_btc = size_usd / self.mid
        bid_vwap = self.getBidVwap(size_btc, True)
        ask_vwap = self.getAskVwap(size_btc, True)
        return ask_vwap - bid_vwap

    def getVWSpreadBps(self, size_usd):
        size_btc = size_usd / self.mid
        vw_spread = self.getVWSpreadAbs(size_btc)
        return vw_spread / self.mid * np.float(10000.0)

    def getBidVwap(self, size_usd, is_executable=False):
        cum_size = np.float(0.0)
        int_sizes = np.array([])
        size_btc = size_usd / self.mid

        for i in range(len(self.bid_sizes)):
            if (self.bid_sizes[i] + cum_size) < size_btc:
                int_sizes = np.append(int_sizes, self.bid_sizes[i])
                cum_size += self.bid_sizes[i]
            else:
                int_sizes = np.append(int_sizes, size_btc - cum_size)
                cum_size += size_btc - cum_size

        if is_executable and (cum_size < size_btc):
            return np.float('nan')
        else:
            return np.average(self.bid_prices, weights=int_sizes)

    def getAskVwap(self, size_usd, is_executable=False):
        cum_size = np.float(0.0)
        int_sizes = np.array([])
        size_btc = size_usd / self.mid
        for i in range(len(self.ask_sizes)):
            if (self.ask_sizes[i] + cum_size) < size_btc:
                int_sizes = np.append(int_sizes, self.ask_sizes[i])
                cum_size += self.ask_sizes[i]
            else:
                int_sizes = np.append(int_sizes, size_btc - cum_size)
                cum_size += size_btc - cum_size

        if is_executable and (cum_size < size_btc):
            return np.float('nan')
        else:
            return np.average(self.ask_prices, weights=int_sizes)

    def getTotalVolume(self):
        return self.total_ask_size + self.total_bid_size

    def getBidDispersion(self):
        return np.std(self.bid_sizes/self.total_bid_size)

    def getAskDispersion(self):
        return np.std(self.ask_sizes/self.total_ask_size)

    def getVWMid(self, size_usd):
        # Calculate weighted mid part 2
        bid_vwap = self.getBidVwap(size_usd, False)
        ask_vwap = self.getAskVwap(size_usd, False)
        return ((bid_vwap * size_usd) + (ask_vwap * size_usd)) / (size_usd * 2.0)
