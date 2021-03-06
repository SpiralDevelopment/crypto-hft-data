# Crypto HFT Data Aggregator
> Crypto High Frequency Data Aggregator

This project helps you to collect real-time orderbook, trade and other HFT data from several crypto exchanges using websocket endpoints.

## Available Exchanges
* Binance
* Bitmex
* Okex
* Bitfinex
* Coinbase
* Bitstamp
* Kraken

## Usage
### 1. Clone, Set-up virtual environment and Install requirements

```bash
$ git clone git@github.com:SpiralDevelopment/crypto-hft-data.git
$ cd crypto-hft-data
$ pip3 install virtualenv
$ virtualenv env
$ source env/bin/activate
$ pip3 install -r requirements.txt
```

### 2. Prepare config file

Decide on what kind of data you want to receive from an exchange, and save that information in json format into [configs.json](https://github.com/SpiralDevelopment/crypto-hft-data/blob/master/configs.json) file.

Example to collect orderbook and trade data from Coinbase for BTC-USD and ETH-USD pairs:
```json
{
  "coinbase": {
    "BTC-USD": "matches,level2",
    "ETH-USD": "matches,level2"
  }
}
```

Example to collect orderbook, trade and liquidation data from Bitmex for XBT-USD and ETH-USD pairs:

```json
{
  "bitmex": {
    "xbtusd": "orderBookL2,trade,liquidation",
    "ethusd": "orderBookL2,trade,liquidation"
  }
}
```

To find out the exact endpoints, you need to refer to the offical API documentations of the exchanges. 

I've already made a sample configurations for all exchanges in the [configs.json](https://github.com/SpiralDevelopment/crypto-hft-data/blob/master/configs.json) file. All those endpoints are active and working as of writing this.

### 3. Launch

Run main.py passing exchange name as an argument.

```bash
python3 main.py -e coinbase
```

All data is saved in .csv file format under ```files/"exchange-name"/"endpoint-name"``` folders.

## Contributions

Contributions and feature requests are always welcome!

## Support

If you like my work, feel free to support it!

* **BTC:** 1PUGs6mxcW2W3SJi95aG8GvRQRJsoFHWWQ

* **ETH:** 0x66615e09f7f46429e7620ffbf78479879bbab41d

* **LTC:** LRxYMgEXMumwxYdimZo9EJ5CfBcipD5c3n

## License
[MIT License](https://github.com/SpiralDevelopment/crypto-hft-data/blob/master/LICENSE)
