# Project Learnings: Polymarket Signal System (PSS)

Building PSS over the last two weeks has been focused on building a production-ready pipeline. Here are my key takeaways from the perspective of an intern trying to solve a real investment problem.

## 1. Researching the "Investment Lens"
Before writing code, I had to understand how a fund like BIT Capital actually thinks. I researched the fund's public holdings and realized that "relevance" isn't just about matching a name. It’s about the causal chain. 
*   **What I learned:** A market about "US-China trade tariffs" is a semiconductor signal, even if NVIDIA isn't mentioned in the title. I built a `holdings.py` configuration that isn't just a list of tickers, but a context-rich map of sectors, macro themes and tags.

## 2. The Two-Pass Architecture (Cost vs. Precision)
One of my biggest challenges was the sheer volume of Polymarket data (~10k events about ~40k markets). Feeding everything to a high-reasoning LLM would have been incredibly expensive and slow.
*   **The Solution:** I implemented a "Gatekeeper" (Pass 1) and a "Senior Analyst" (Pass 2).
*   **The Learning:** By using a cheaper model with a strict prompt for Pass 1, I could discard 60% of the noise before spending expensive tokens on Pass 2 for deep causal analysis. This showed me again that in AI engineering, data engineering and architecture is often more important than the model itself.

## 3. Dealing with Market Noise
I could just use keyword matching for filtering. I quickly realized that many markets use "AI" as a buzzword but have zero impact on equity values.
*   **What I learned:** I had to implement a multi-stage funnel. First, category-based filter, then numeric filters (liquidity/volume) to ensure the market is "real," then a tag-based relevance check (requiring 50% tag intersection), and only then the LLM. Traditional logic and AI should work together, not in isolation.

## 4. The Complexity of "Sentiment"
Quantifying a market event into a "Bullish" or "Bearish" signal is harder than it looks. A "Yes" outcome for a tariff market might be bearish for one stock but bullish for a competitor.
*   **The Learning:** I learned to prompt the LLM to trace the *reasoning* before committing to a direction. If the LLM can't explain the "why," the signal is useless to an analyst. This is why I added the `foundational_details` and `circumstances` fields to make the AI's "black box" transparent.
 

## 5. If I had more time...
If I had another two weeks, I would:
1.  **Implement Feedback Loops:** Let analysts "thumbs up/down" a signal in the Streamlit UI to re-train the LLM prompts.
2.  **Backtesting:** Compare the PSS "Bullish" signals against actual stock price movements to see if the prediction markets are leading or lagging indicators.
3.  **Entity Resolution:** Use a more robust way to map weirdly named Polymarket events to exact Bloomberg-style tickers.

---
**Summary:** This project showed me that the "AI" part is less than 5% of the work - the other 95%+ is data engineering, infrastructure, and deeply understanding the domain you are building for.
