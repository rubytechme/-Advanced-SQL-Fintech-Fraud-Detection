# Fintech Transaction Analytics - Advanced SQL Project

## 🎯 Introduction: 
This project demonstrates advanced SQL analytics for a digital payments and fintech platform. Built for a fictional digital wallet and lending company, the analysis covers fraud detection, risk management, customer lifetime value, and cross-border payment analytics.

The project showcases production-grade SQL used in modern fintech companies like PayPal, Stripe, Revolut, and Cash App to detect fraud in real-time, assess credit risk, optimize payment routing, and calculate revenue metrics.

Dataset Scope:

- Users: Customer profiles with KYC status and account tiers
- Wallets: Multi-currency digital wallets
- Transactions: P2P transfers, merchant payments, bill payments
- Fraud Flags: Risk indicators and investigation status
- Loan Applications: Credit scoring and default risk assessment


## 🔍 Problem Statement: 
Fintech companies face several critical operational challenges:

1. Fraud Detection: Real-time identification of suspicious transaction patterns before funds are lost
2. Credit Risk: Assessing loan default probability to minimize losses
3. Revenue Optimization: Calculating customer lifetime value across multiple revenue streams (fees, interest, subscriptions)
4. Payment Performance: Monitoring transaction success rates and optimizing routing
5. Cross-Border Analytics: Understanding international payment corridors and FX revenue
6. User Engagement: Tracking DAU/MAU and retention metrics for product teams

This project builds SQL-based solutions for each of these challenges using real-world fintech methodologies.

## 💡 Skills & Concepts Demonstrated:
#### Advanced SQL Techniques:

- ✅ Window Functions with RANGE BETWEEN: Time-based aggregations for fraud detection
- ✅ LAG/LEAD: Geographic anomaly detection (impossible travel)
- ✅ Multiple CTEs: Complex multi-step revenue calculations
- ✅ Risk Scoring Logic: Multi-factor decisioning with CASE statements
- ✅ Time Series Analysis: DAU/MAU, WoW growth, retention cohorts
- ✅ Statistical Calculations: DTI ratios, payment-to-income ratios
- ✅ Cross-Table Joins: Complex financial reconciliation
- ✅ Strategic Indexing: Performance optimization for high-volume queries

#### Fintech Business Concepts:

- Transaction velocity checks (fraud prevention)
- Geographic anomaly detection (account takeover)
- Customer Lifetime Value (CLV) calculation
- Credit risk scoring and default prediction
- DAU/MAU ratio and growth metrics
- Cross-border payment corridor analysis
- KYC compliance tracking


## 🚀 Main Insights & Code:
### 1️⃣ Real-Time Fraud Detection - Velocity Check
Business Value: Identifies suspicious transaction patterns in real-time by analyzing velocity (how fast/frequent transactions occur) and amount anomalies.

```bash
WITH transaction_velocity AS (
    SELECT 
        t.transaction_id,
        t.sender_wallet_id,
        w.user_id,
        u.username,
        t.amount,
        t.transaction_date,
        -- Count transactions in last 1 hour
        COUNT(*) OVER (
            PARTITION BY t.sender_wallet_id 
            ORDER BY t.transaction_date
            RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW
        ) AS transactions_last_hour,
        -- Sum amount in last 1 hour
        SUM(t.amount) OVER (
            PARTITION BY t.sender_wallet_id 
            ORDER BY t.transaction_date
            RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW
        ) AS amount_last_hour,
        -- Average transaction amount for user
        AVG(t.amount) OVER (PARTITION BY t.sender_wallet_id) AS user_avg_transaction
    FROM transactions t
    JOIN wallets w ON t.sender_wallet_id = w.wallet_id
    JOIN users u ON w.user_id = u.user_id
    WHERE t.transaction_status = 'Completed'
),
risk_assessment AS (
    SELECT 
        *,
        -- Multi-factor risk scoring
        CASE 
            WHEN transactions_last_hour > 10 THEN 30
            WHEN transactions_last_hour > 5 THEN 20
            WHEN transactions_last_hour > 3 THEN 10
            ELSE 0
        END +
        CASE 
            WHEN amount > user_avg_transaction * 5 THEN 30
            WHEN amount > user_avg_transaction * 3 THEN 20
            WHEN amount > user_avg_transaction * 2 THEN 10
            ELSE 0
        END +
        CASE 
            WHEN amount_last_hour > 10000 THEN 25
            WHEN amount_last_hour > 5000 THEN 15
            ELSE 0
        END AS risk_score
    FROM transaction_velocity
)
SELECT 
    transaction_id,
    username,
    amount,
    transaction_date,
    transactions_last_hour,
    ROUND(amount_last_hour, 2) AS amount_last_hour,
    risk_score,
    CASE 
        WHEN risk_score >= 50 THEN 'High Risk - Block'
        WHEN risk_score >= 30 THEN 'Medium Risk - Review'
        WHEN risk_score >= 15 THEN 'Low Risk - Monitor'
        ELSE 'Normal'
    END AS risk_category
FROM risk_assessment
WHERE risk_score > 0
ORDER BY risk_score DESC;
```

### Key Insights:

Uses RANGE BETWEEN INTERVAL for time-based windows (more accurate than ROWS for timestamps)
Multi-factor risk scoring combines velocity + amount + total exposure
Real-time actionable categories: Block, Review, Monitor, Normal


### 2️⃣ Geographic Anomaly Detection (Impossible Travel)
Business Value: Detects account takeover by identifying physically impossible travel patterns (e.g., transactions from USA and China 2 hours apart).

```bash
WITH user_locations AS (
    SELECT 
        t.transaction_id,
        w.user_id,
        u.username,
        t.transaction_date,
        t.country_code,
        t.amount,
        LAG(t.country_code) OVER (
            PARTITION BY w.user_id ORDER BY t.transaction_date
        ) AS previous_country,
        LAG(t.transaction_date) OVER (
            PARTITION BY w.user_id ORDER BY t.transaction_date
        ) AS previous_transaction_date,
        EXTRACT(EPOCH FROM (
            t.transaction_date - LAG(t.transaction_date) OVER (
                PARTITION BY w.user_id ORDER BY t.transaction_date
            )
        ))/3600 AS hours_since_last_transaction
    FROM transactions t
    JOIN wallets w ON t.sender_wallet_id = w.wallet_id
    JOIN users u ON w.user_id = u.user_id
    WHERE t.transaction_status = 'Completed'
),
geographic_anomalies AS (
    SELECT 
        *,
        CASE 
            WHEN previous_country IS NOT NULL 
            AND country_code != previous_country 
            AND hours_since_last_transaction < 4 
            THEN 'Impossible Travel'
            WHEN previous_country IS NOT NULL 
            AND country_code != previous_country 
            AND hours_since_last_transaction < 12 
            THEN 'Suspicious Travel'
            ELSE 'Normal'
        END AS anomaly_type
    FROM user_locations
)
SELECT 
    username,
    transaction_id,
    previous_country,
    country_code AS current_country,
    ROUND(hours_since_last_transaction, 2) AS hours_between_transactions,
    amount,
    anomaly_type
FROM geographic_anomalies
WHERE anomaly_type != 'Normal'
ORDER BY hours_since_last_transaction ASC;
```

### Key Insights:

LAG function tracks previous location per user
EXTRACT(EPOCH) converts time differences to hours
Impossible travel flagged if location changes too quickly


### 3️⃣ Customer Lifetime Value (CLV) - Multi-Revenue Stream
Business Value: Calculates total revenue per customer across transaction fees, loan interest, and subscriptions.

```bash
WITH transaction_revenue AS (
    SELECT 
        w.user_id,
        u.username,
        u.account_tier,
        COUNT(DISTINCT t.transaction_id) AS total_transactions,
        SUM(t.amount * 0.015) AS transaction_fee_revenue
    FROM transactions t
    JOIN wallets w ON t.sender_wallet_id = w.wallet_id
    JOIN users u ON w.user_id = u.user_id
    WHERE t.transaction_status = 'Completed'
    GROUP BY w.user_id, u.username, u.account_tier
),
loan_revenue AS (
    SELECT 
        user_id,
        SUM(loan_amount * (interest_rate/100) * (loan_term_months/12.0)) AS total_interest_revenue
    FROM loan_applications
    WHERE loan_status IN ('Disbursed', 'Repaid')
    GROUP BY user_id
),
subscription_revenue AS (
    SELECT 
        user_id,
        CASE 
            WHEN account_tier = 'Premium' THEN 10
            WHEN account_tier = 'Business' THEN 25
            ELSE 0
        END * EXTRACT(MONTH FROM AGE(CURRENT_DATE, registration_date)) AS subscription_revenue
    FROM users
),
customer_ltv AS (
    SELECT 
        tr.user_id,
        tr.username,
        ROUND(COALESCE(tr.transaction_fee_revenue, 0) + 
              COALESCE(lr.total_interest_revenue, 0) + 
              COALESCE(sr.subscription_revenue, 0), 2) AS total_customer_ltv
    FROM transaction_revenue tr
    LEFT JOIN loan_revenue lr ON tr.user_id = lr.user_id
    LEFT JOIN subscription_revenue sr ON tr.user_id = sr.user_id
)
SELECT 
    username,
    total_customer_ltv,
    RANK() OVER (ORDER BY total_customer_ltv DESC) AS customer_value_rank
FROM customer_ltv
ORDER BY total_customer_ltv DESC;
```

### Key Insights:

Three revenue streams: transaction fees (1.5%), loan interest, subscriptions
Separate CTEs for each revenue type, then combined
Enables customer segmentation by profitability


### 4️⃣ Loan Default Risk Prediction
Business Value: Assesses credit risk using multiple factors to approve/reject loans automatically.

```bash
WITH loan_metrics AS (
    SELECT 
        la.loan_id,
        la.username,
        la.loan_amount,
        la.credit_score,
        la.monthly_income,
        la.employment_status,
        -- Debt-to-income ratio
        la.loan_amount / (la.monthly_income * la.loan_term_months) AS dti_ratio,
        -- Payment-to-income ratio
        ((la.loan_amount * (1 + la.interest_rate/100 * la.loan_term_months/12.0)) / la.loan_term_months) / la.monthly_income AS payment_to_income_ratio,
        -- Recent transaction activity
        COUNT(t.transaction_id) AS transaction_count_last_90_days
    FROM loan_applications la
    LEFT JOIN transactions t ON la.user_id = t.sender_wallet_id
        AND t.transaction_date >= la.application_date - INTERVAL '90 days'
    GROUP BY la.loan_id, [other columns]
),
risk_scoring AS (
    SELECT 
        *,
        -- Multi-factor credit risk score
        (CASE WHEN credit_score < 600 THEN 30 ELSE 0 END) +
        (CASE WHEN payment_to_income_ratio > 0.40 THEN 20 ELSE 0 END) +
        (CASE WHEN employment_status = 'Unemployed' THEN 20 ELSE 0 END) +
        (CASE WHEN transaction_count_last_90_days < 5 THEN 10 ELSE 0 END) 
        AS default_risk_score
    FROM loan_metrics
)
SELECT 
    loan_id,
    username,
    loan_amount,
    credit_score,
    default_risk_score,
    CASE 
        WHEN default_risk_score >= 60 THEN 'High Risk - Reject'
        WHEN default_risk_score >= 40 THEN 'Medium Risk - Manual Review'
        ELSE 'Low Risk - Approve'
    END AS recommendation
FROM risk_scoring
ORDER BY default_risk_score DESC;
```

### Key Insights:

DTI (debt-to-income) ratio: key lending metric
Transaction history indicates account health
Automated decisioning reduces manual review costs


### 5️⃣ DAU/MAU and Growth Metrics
Business Value: Tracks user engagement for product teams, calculates week-over-week growth.

```bash
WITH daily_active_users AS (
    SELECT 
        DATE(transaction_date) AS activity_date,
        COUNT(DISTINCT user_id) AS dau,
        SUM(amount) AS daily_volume
    FROM transactions
    WHERE transaction_status = 'Completed'
    GROUP BY DATE(transaction_date)
),
monthly_active_users AS (
    SELECT 
        DATE_TRUNC('month', transaction_date) AS activity_month,
        COUNT(DISTINCT user_id) AS mau
    FROM transactions
    WHERE transaction_status = 'Completed'
    GROUP BY DATE_TRUNC('month', transaction_date)
)
SELECT 
    dau.activity_date,
    dau.dau,
    mau.mau,
    ROUND((dau.dau::DECIMAL / mau.mau) * 100, 2) AS dau_mau_ratio,
    LAG(dau.dau, 7) OVER (ORDER BY activity_date) AS dau_7_days_ago,
    ROUND(((dau.dau - LAG(dau.dau, 7) OVER (ORDER BY activity_date)) / 
           LAG(dau.dau, 7) OVER (ORDER BY activity_date)) * 100, 2) AS wow_growth_pct
FROM daily_active_users dau
LEFT JOIN monthly_active_users mau 
    ON DATE_TRUNC('month', dau.activity_date) = mau.activity_month
ORDER BY activity_date DESC;
```

### Key Insights:

DAU/MAU ratio indicates product stickiness (>20% is good)
WoW (week-over-week) growth tracks momentum
Essential for product-market fit assessment


### 6️⃣ Cross-Border Payment Corridor Analysis
Business Value: Identifies high-value international payment routes and calculates FX revenue.

```bash
WITH cross_border_transactions AS (
    SELECT 
        sender_country,
        receiver_country,
        COUNT(*) AS transaction_count,
        SUM(amount) AS total_volume,
        SUM(amount * 0.03) AS fx_revenue  -- 3% FX markup
    FROM transactions t
    JOIN users sender ON t.sender_wallet_id = sender.wallet_id
    JOIN users receiver ON t.receiver_wallet_id = receiver.wallet_id
    WHERE sender.country != receiver.country
    GROUP BY sender_country, receiver_country
)
SELECT 
    sender_country || ' → ' || receiver_country AS corridor,
    transaction_count,
    ROUND(total_volume, 2) AS total_volume,
    ROUND(fx_revenue, 2) AS fx_revenue,
    RANK() OVER (ORDER BY total_volume DESC) AS corridor_rank
FROM cross_border_transactions
ORDER BY total_volume DESC;
```

### Key Insights:

Identifies top remittance corridors (e.g., USA → Mexico)
FX revenue is a major profit center for fintech companies
Enables strategic partnerships with payment networks


## 🚧 Challenges Encountered:
#### Challenge 1: Time-Based Window Frames for Fraud Detection
- Issue: Using ROWS BETWEEN doesn't work well for timestamp-based fraud detection because transactions don't occur at regular intervals.
- Solution: Used RANGE BETWEEN INTERVAL '1 hour' PRECEDING which properly handles timestamp ranges regardless of how many transactions occurred.
  
#### Challenge 2: Handling NULL Values in LAG for New Users
- Issue: First transaction for each user has NULL for previous_country, causing errors in geographic anomaly detection.
- Solution: Added WHERE previous_country IS NOT NULL condition to only flag anomalies after at least one previous transaction.
  
#### Challenge 3: Multi-Revenue Stream Aggregation
- Issue: Users might have transaction revenue but no loans, or vice versa, causing missing data in JOINs.
- Solution: Used LEFT JOINs with COALESCE to handle missing revenue streams gracefully.

## Business Applications:

- Fraud Teams: Real-time risk scoring and geographic anomaly detection
- Credit Risk: Automated loan decisioning with explainable risk factors
- Finance Teams: Accurate CLV calculation across revenue streams
- Product Teams: User engagement metrics and growth tracking
- Operations: Payment success rate monitoring and routing optimization

## Key Takeaways:
✅ RANGE BETWEEN INTERVAL is essential for time-based fraud detection
✅ LAG/LEAD enable sequential analysis for anomaly detection
✅ Multi-CTE patterns improve readability for complex business logic
✅ Risk scoring combines multiple signals for better decisions
✅ Strategic indexing dramatically improves query performance

## 🛠 How to Use:
#### Prerequisites:
- PostgreSQL 12+
- Understanding of fintech terminology (CLV, DAU/MAU, DTI, etc.)

#### Setup:
# Create database
createdb fintech_analytics

# Run SQL file
psql -d fintech_analytics -f fintech_analytics.sql

⭐ Star this repo if you found it helpful for your data analytics journey!
