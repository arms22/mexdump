# -*- coding: utf-8 -*-
import argparse
import datetime
import dateutil.parser
from pytz import timezone
import time
import ccxt
import pandas as pd

def store_datetime(str):
    return dateutil.parser.parse(str)

parser = argparse.ArgumentParser(description="")
parser.add_argument("--symbol", type=str, default='XBTUSD')
parser.add_argument("--start", type=store_datetime)
parser.add_argument("--end", type=store_datetime)
parser.add_argument("--bucketed", type=str, default=None)
parser.add_argument("--quote", action="store_true")
parser.add_argument("--funding", action="store_true")
args = parser.parse_args()

api = ccxt.bitmex()

api_args = {}
api_args['symbol'] = args.symbol
api_args['count'] = 500
api_args['start'] = 0
if args.start is not None:
    api_args['startTime'] = args.start
    api_args['reverse'] = 'false'
else:
    api_args['reverse'] = 'true'

resample_info = { 'needs': False }
if args.bucketed is not None:
    resample_info_for_binsize = {
        '1m': { 'bin' : '1m', 'needs': False, 'count': 500 },
        '3m': { 'bin' : '1m', 'needs': True, 'count': 450 },
        '5m': { 'bin' : '5m', 'needs': False, 'count': 500 },
        '15m': { 'bin' : '5m', 'needs': True, 'count': 300 },
        '30m': { 'bin' : '5m', 'needs': True, 'count': 300 },
        '45m': { 'bin' : '5m', 'needs': True, 'count': 450 },
        '1h': { 'bin' : '1h', 'needs': False, 'count': 500 },
        '2h': { 'bin' : '1h', 'needs': True, 'count': 500 },
        '4h': { 'bin' : '1h', 'needs': True, 'count': 400 },
        '1d': { 'bin' : '1d', 'needs': False, 'count': 500 },
    }
    resample_info = resample_info_for_binsize[args.bucketed]
    api_args['binSize'] = resample_info['bin']
    api_args['count'] = resample_info['count']

header = True

while True:
    try:
        if args.funding:
            messages = api.publicGetFunding(api_args)
        elif args.quote:
            if args.bucketed:
                messages = api.publicGetQuoteBucketed(api_args)
            else:
                messages = api.publicGetQuote(api_args)
        else:
            if args.bucketed:
                messages = api.publicGetTradeBucketed(api_args)
            else:
                messages = api.publicGetTrade(api_args)

        df = pd.DataFrame(messages)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)

        if len(df.index):
            api_args['startTime'] = df.index[-1] + datetime.timedelta(microseconds=10000)

        if resample_info['needs']:
            rule = args.bucketed
            rule = rule.replace('m','T')
            rule = rule.replace('d','D')
            df = df.resample(rule=rule, closed='right').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'})

        print(df.to_csv(header=header))
        header = False

        if api_args['reverse'] == 'true':
            break
        else:
            if args.end is not None:
                if api_args['startTime'] >= args.end:
                    break
            if len(df.index) < api_args['count']:
                break

    except Exception as e:
        print(e)

    time.sleep(3)
