"""
Hardcoded list of BIT Capital holdings and focus areas for LLM classification.
This acts as the source of truth for relevance checking in the LLM pipeline.
"""

import json

BIT_CAPITAL_HOLDINGS = {
    "tickers": {
        "NVDA": "NVIDIA - AI chips, GPU compute, hyperscaler capex, US-China semiconductor export controls. World's dominant AI chip supplier.",
        "AMD": "Advanced Micro Devices - AI chips, data center GPUs, direct NVDA competitor, US-China export controls.",
        "TSMC": "Taiwan Semiconductor Manufacturing Co - World's largest chip foundry, geopolitical risk (Taiwan-China), AI supply chain chokepoint.",
        "MU": "Micron Technology - Memory chips (DRAM, NAND), AI infrastructure memory demand, US-China export controls.",
        "AMZN": "Amazon - AWS cloud hyperscaler, AI infrastructure capex, consumer spending bellwether.",
        "IREN": "Iris Energy - Bitcoin mining operations, AI data center expansion, renewable energy compute.",
        "HUT": "Hut 8 - Bitcoin mining, high-performance compute infrastructure.",
        "WULF": "TeraWulf - Bitcoin mining powered by zero-carbon nuclear energy.",
        "RIOT": "Riot Platforms - Large-scale Bitcoin mining operations.",
        "BITF": "Bitfarms - Bitcoin mining, global compute infrastructure.",
        "BTC": "Bitcoin - Price milestones, ETF flows, regulation, mining difficulty, macro crypto sentiment.",
        "ETH": "Ethereum - Price milestones, staking, regulation, DeFi ecosystem health.",
        "AUTO1": "Auto1 Group - European used car marketplace, EU auto market health, EU tariffs on Chinese EVs.",
        "HNGE": "Hinge Health - Digital musculoskeletal health, US digital health regulation.",
        "OSCR": "Oscar Health - US health insurance, Affordable Care Act, healthcare policy.",
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
    ],
    "relevant_tags": [
    # AI
    "ai", "anthropic", "claude", "openai", "deepseek", "grok", "gpt-5",
    "google", "automation", "amodei",
    # Semiconductors
    "nvda", "tsmc", "amd", "mu", "micron", "semiconductor", "chips",
    # Crypto
    "bitcoin", "btc", "ethereum", "eth", "crypto", "solana", "dogecoin",
    "token", "airdrops", "defi", "staking", "mining",
    # Crypto miners
    "iren", "hut", "wulf", "riot", "bitf",
    # Hyperscalers
    "aws", "amzn", "amazon", "hyperscaler",
    # Trade / Geopolitics
    "tariffs", "sanctions", "embargo", "china", "taiwan", "nuclear",
    # Crypto macro
    "etf",
]
}


def get_bit_capital_holdings_json() -> str:
    return json.dumps(BIT_CAPITAL_HOLDINGS, indent=2)