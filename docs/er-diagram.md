erDiagram
    USER {
        int user_id PK
        string email
        string password_hash
        string display_name
        datetime created_at
    }
    PLAN {
        int plan_id PK
        string plan_name
        decimal monthly_price
        int max_concurrent_streams
    }
    SUBSCRIPTION {
        int subscription_id PK
        int user_id FK
        int plan_id FK
        string status
        date start_date
        date end_date
    }
    PAYMENT {
        int payment_id PK
        int user_id FK
        int subscription_id FK
        int bet_id FK
        decimal amount
        string currency
        string status
        enum direction "debit, credit"
        string payment_method
        string idempotency_key UK
        datetime created_at
    }
    SPORTS_EVENT {
        int sports_event_id PK
        string title
        string sport_type
        datetime start_time
        enum status "scheduled, live, finished, cancelled"
    }
    OUTCOME {
        int outcome_id PK
        int sports_event_id FK
        string description
        enum status "open, won, lost"
    }
    ODDS_SNAPSHOT {
        int snapshot_id PK
        int outcome_id FK, UK
        datetime captured_at UK
        decimal price
    }
    STREAMING_SESSION {
        int session_id PK
        int user_id FK
        int sports_event_id FK
        int subscription_id FK
        datetime started_at
        datetime ended_at
    }
    BET {
        int bet_id PK
        int user_id FK
        int snapshot_id FK
        int streaming_session_id FK
        decimal amount_staked
        enum status "pending, won, lost, voided"
        datetime placed_at
    }

    USER ||--o{ SUBSCRIPTION : subscribes
    USER ||--o{ PAYMENT : makes
    USER ||--o{ BET : places
    USER ||--o{ STREAMING_SESSION : watches
    PLAN ||--o{ SUBSCRIPTION : prices
    SUBSCRIPTION ||--o{ PAYMENT : generates
    SUBSCRIPTION ||--o{ STREAMING_SESSION : "grants access to"
    BET ||--o{ PAYMENT : generates
    SPORTS_EVENT ||--o{ OUTCOME : has
    SPORTS_EVENT ||--o{ STREAMING_SESSION : "is streamed in"
    OUTCOME ||--o{ ODDS_SNAPSHOT : "is priced by"
    ODDS_SNAPSHOT ||--o{ BET : "locks in price for"
    STREAMING_SESSION |o--o{ BET : "is placed during"