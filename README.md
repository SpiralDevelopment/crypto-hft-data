# Crypto HFT Data
> Crytpo High Frequency Data Aggregator

This project helps you to collect real-time orderbook, trade and other HFT data from several crytpo exchanges using websocket endpoints.

## Available Exchanges
* Binance
* Bitmex
* Okex
* Bitfinex
* Coinbase
* Bitstamp
* Kraken

## Run
### 1. Set-Up virtual environment and Install requirements

```bash
$ pip3 install virtualenv
$ virtualenv venv
$ source env/bin/activate
$ pip3 install -r requirements.txt
```

### 2. Prepare config file

Decide on what kind of data you want to receive from an exchange and save that information in json format into [configs.json](https://github.com/SpiralDevelopment/crypto-hft-data/blob/master/configs.json) file.

Example to collect orederbook and trade data from Coinbase for BTC-USD and ETH-USD pairs:
```json
{
  "coinbase": {
    "BTC-USD": "matches,level2",
    "ETH-USD": "matches,level2"
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

All data is saved in .csv file format under files/"exchange-name"/"endpoint-name" folders.


