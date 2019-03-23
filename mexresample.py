import sys
import argparse
import pandas as pd

parser = argparse.ArgumentParser(description="")
parser.add_argument('csv', nargs='?', type=argparse.FileType('r'), default=sys.stdin)
parser.add_argument("--rule", dest='rule', type=str, default='1T')
args = parser.parse_args()

df = pd.read_csv(args.csv, index_col="timestamp", parse_dates=True)

ohlc = df['price'].resample(args.rule).ohlc()
volume = df['size'].resample(args.rule).sum()

sell = df[df['side']=='Sell']
sell_volume = sell['size'].resample(args.rule).sum()

buy = df[df['side']=='Buy']
buy_volume = buy['size'].resample(args.rule).sum()

data_ohlc = pd.DataFrame({'open': ohlc.open, 'high': ohlc.high, 'low': ohlc.low, 'close': ohlc.low, 'volume':volume, 'buy_volume':buy_volume, 'sell_volume':sell_volume})
data_ohlc = data_ohlc.dropna()
print(data_ohlc.to_csv())
