"""
Hardcoded list of BIT Capital holdings and focus areas for LLM classification.
This acts as the source of truth for relevance checking in the LLM pipeline.
"""

import json

BIT_CAPITAL_HOLDINGS = {
    "tickers": {
        "NVDA": "NVIDIA - AI chips, hyperscaler capex, US-China export controls",
        "AMD": "Advanced Micro Devices - AI chips, data center, US-China export controls",
        "TSMC": "Taiwan Semiconductor Manufacturing Co - Foundry, geopolitics, AI supply chain",
        "MU": "Micron Technology - Memory chips, AI infrastructure",
        "AMZN": "Amazon - AWS, hyperscaler capex, consumer spending",
        "IREN": "Iris Energy - Bitcoin mining, AI data centers",
        "HUT": "Hut 8 - Bitcoin mining, compute infrastructure",
        "WULF": "TeraWulf - Bitcoin mining, zero-carbon energy",
        "RIOT": "Riot Platforms - Bitcoin mining",
        "BITF": "Bitfarms - Bitcoin mining",
        "BTC": "Bitcoin milestone prices and regulation",
        "ETH": "Ethereum milestone prices and regulation",
        "AUTO1": "Auto1 Group - EU auto market, tariffs",
        "HNGE": "Hinge Health - Digital health regulation",
        "OSCR": "Oscar Health - Healthcare policy",
    },
    "sectors": [
        "Semiconductors",
        "AI Infrastructure",
        "Crypto Mining",
        "Hyperscalers",
        "US Macro",
        "Geopolitics (US-China / Taiwan)",
        "European Auto Industry",
        "Digital Health",
    ],
    "macro_themes": [
        "Fed Rate Cuts",
        "US Presidential Election",
        "Semiconductor Export Controls",
        "Hyperscaler Capex Trends",
        "Bitcoin Spot ETF Flows",
        "EU Tariffs on Chinese EVs",
    ]
}


def get_bit_capital_holdings_json() -> str:
    return json.dumps(BIT_CAPITAL_HOLDINGS, indent=2)