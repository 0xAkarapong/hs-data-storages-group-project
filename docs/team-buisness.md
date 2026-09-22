# Team

- Member 1: Akarapong (Drake)
- Member 2: Patthadon (Arm)
- Member 3: Chitipat (Palm)

# Business Idea

Our business is an online platform that combines **live sports streaming** with **real-time sports betting** in one place. Users can watch live football, basketball, and other sporting events while viewing changing odds and placing bets without leaving the stream.

The customers are sports fans who want both entertainment and betting in the same experience. Users pay through a **monthly streaming subscription**, while the platform also earns revenue from the **margin on bets**.

At scale, the platform can process **millions of odds updates, bets, and stream-viewing sessions every day**. The largest data comes from live betting because each betting market can receive many new odds values while a match is being played.

# Business Logic ER Diagram

- **USER**: The person using the platform to watch live sports and place bets
- **SUBSCRIPTION**: A user's paid monthly plan that grants access to streaming
- **PAYMENT**: A completed charge, coming from either a subscription renewal or a bet
- **SPORTS_EVENT**: A single live match or game available on the platform, such as one football or basketball fixture
- **OUTCOME**: One specific result within a sports event that can be bet on, such as "Team A wins" or "Over 2.5 goals"
- **ODDS_SNAPSHOT**: The price for one outcome at one exact moment, capturing how the odds move as the match plays out
- **BET**: A wager a user places on a specific outcome, tied to the streaming session they placed it during
- **STREAMING_SESSION**: One instance of a user watching the live stream of a sports event

# Expected Order of Magnitude

| Entity | Expected Size | Growth |
|---|---:|---:|
| User | 100K to 1M total users | 1K to 10K new users per day |
| Subscription | 100K to 500K active subscriptions | Roughly 500 to 3K per day, tracking new signups minus churn |
| SportsEvent | 1K to 5K events stored | 10 to 100 new events per day |
| StreamSession | 5M to 20M sessions per day | Tracks active viewers across concurrent live matches |
| OddsSnapshot | 10M to 50M new rows per day | Can spike far higher during marquee live matches with many concurrently active markets |
| Bet | 100K to 1M bets placed per day | Can spike several times higher during popular live events |