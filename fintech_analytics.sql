-- ============================================================================
-- FINTECH TRANSACTION ANALYTICS - ADVANCED SQL PROJECT
-- Author: Ruby
-- Database: PostgreSQL
-- Purpose: Digital payment fraud detection, customer behavior, and revenue analytics
-- ============================================================================

-- ============================================================================
-- SECTION 1: DATABASE SETUP & SCHEMA CREATION
-- ============================================================================

-- Create users table
CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(100),
    email VARCHAR(150),
    phone_number VARCHAR(20),
    country VARCHAR(50),
    registration_date DATE,
    kyc_status VARCHAR(20), -- 'Verified', 'Pending', 'Rejected'
    account_tier VARCHAR(20), -- 'Basic', 'Premium', 'Business'
    date_of_birth DATE,
    is_active BOOLEAN DEFAULT TRUE
);

-- Create wallets table
CREATE TABLE wallets (
    wallet_id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(user_id),
    wallet_type VARCHAR(20), -- 'Main', 'Savings', 'Investment'
    currency VARCHAR(10),
    balance DECIMAL(15,2),
    created_date DATE,
    last_transaction_date DATE
);

-- Create transactions table
CREATE TABLE transactions (
    transaction_id SERIAL PRIMARY KEY,
    sender_wallet_id INT REFERENCES wallets(wallet_id),
    receiver_wallet_id INT REFERENCES wallets(wallet_id),
    transaction_type VARCHAR(30), -- 'P2P', 'Bill_Payment', 'Merchant', 'Withdrawal', 'Deposit'
    amount DECIMAL(15,2),
    currency VARCHAR(10),
    transaction_date TIMESTAMP,
    transaction_status VARCHAR(20), -- 'Completed', 'Failed', 'Pending', 'Reversed'
    merchant_category VARCHAR(50),
    device_type VARCHAR(20), -- 'Mobile', 'Web', 'API'
    ip_address VARCHAR(50),
    country_code VARCHAR(5)
);

-- Create fraud_flags table
CREATE TABLE fraud_flags (
    flag_id SERIAL PRIMARY KEY,
    transaction_id INT REFERENCES transactions(transaction_id),
    flag_type VARCHAR(50), -- 'Velocity', 'Amount_Anomaly', 'Geographic', 'Device_Change'
    risk_score DECIMAL(5,2),
    flagged_date TIMESTAMP,
    investigation_status VARCHAR(20) -- 'Open', 'Cleared', 'Confirmed_Fraud'
);

-- Create loan_applications table
CREATE TABLE loan_applications (
    loan_id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(user_id),
    loan_amount DECIMAL(15,2),
    interest_rate DECIMAL(5,2),
    loan_term_months INT,
    application_date DATE,
    approval_date DATE,
    loan_status VARCHAR(20), -- 'Approved', 'Rejected', 'Disbursed', 'Repaid', 'Defaulted'
    credit_score INT,
    monthly_income DECIMAL(15,2),
    employment_status VARCHAR(30)
);

-- ============================================================================
-- SECTION 2: FRAUD DETECTION & RISK ANALYTICS
-- ============================================================================

-- ----------------------------------------------------------------------------
-- QUERY 1: Real-Time Fraud Detection - Velocity Check
-- Demonstrates: Window Functions, RANGE BETWEEN, Risk Scoring
-- ----------------------------------------------------------------------------

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
        -- Calculate risk score based on velocity and amount anomalies
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
    ROUND(user_avg_transaction, 2) AS user_avg_transaction,
    risk_score,
    CASE 
        WHEN risk_score >= 50 THEN 'High Risk - Block'
        WHEN risk_score >= 30 THEN 'Medium Risk - Review'
        WHEN risk_score >= 15 THEN 'Low Risk - Monitor'
        ELSE 'Normal'
    END AS risk_category
FROM risk_assessment
WHERE risk_score > 0
ORDER BY risk_score DESC, transaction_date DESC;


-- ----------------------------------------------------------------------------
-- QUERY 2: Geographic Anomaly Detection (Impossible Travel)
-- Demonstrates: LAG, Time Calculations, Anomaly Detection
-- ----------------------------------------------------------------------------

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
    transaction_date,
    previous_country,
    country_code AS current_country,
    ROUND(hours_since_last_transaction, 2) AS hours_between_transactions,
    amount,
    anomaly_type
FROM geographic_anomalies
WHERE anomaly_type != 'Normal'
ORDER BY hours_since_last_transaction ASC;


-- ----------------------------------------------------------------------------
-- QUERY 3: Customer Lifetime Value (CLV) for Fintech
-- Demonstrates: Multiple CTEs, Revenue Aggregations, Business Metrics
-- ----------------------------------------------------------------------------

WITH transaction_revenue AS (
    SELECT 
        w.user_id,
        u.username,
        u.account_tier,
        u.registration_date,
        COUNT(DISTINCT t.transaction_id) AS total_transactions,
        SUM(t.amount) AS total_transaction_volume,
        SUM(t.amount * 0.015) AS transaction_fee_revenue,
        MAX(t.transaction_date) AS last_transaction_date
    FROM transactions t
    JOIN wallets w ON t.sender_wallet_id = w.wallet_id
    JOIN users u ON w.user_id = u.user_id
    WHERE t.transaction_status = 'Completed'
    GROUP BY w.user_id, u.username, u.account_tier, u.registration_date
),
loan_revenue AS (
    SELECT 
        user_id,
        COUNT(*) AS total_loans,
        SUM(loan_amount) AS total_loan_amount,
        SUM(loan_amount * (interest_rate/100) * (loan_term_months/12.0)) AS total_interest_revenue
    FROM loan_applications
    WHERE loan_status IN ('Disbursed', 'Repaid')
    GROUP BY user_id
),
subscription_revenue AS (
    SELECT 
        user_id,
        account_tier,
        CASE 
            WHEN account_tier = 'Premium' THEN 10
            WHEN account_tier = 'Business' THEN 25
            ELSE 0
        END * EXTRACT(MONTH FROM AGE(CURRENT_DATE, registration_date)) AS subscription_revenue
    FROM users
    WHERE account_tier IN ('Premium', 'Business')
),
customer_ltv AS (
    SELECT 
        tr.user_id,
        tr.username,
        tr.account_tier,
        tr.registration_date,
        EXTRACT(MONTH FROM AGE(CURRENT_DATE, tr.registration_date)) AS months_active,
        tr.total_transactions,
        ROUND(tr.total_transaction_volume, 2) AS total_transaction_volume,
        ROUND(tr.transaction_fee_revenue, 2) AS transaction_fee_revenue,
        COALESCE(lr.total_loans, 0) AS total_loans,
        ROUND(COALESCE(lr.total_interest_revenue, 0), 2) AS loan_interest_revenue,
        ROUND(COALESCE(sr.subscription_revenue, 0), 2) AS subscription_revenue,
        ROUND(
            COALESCE(tr.transaction_fee_revenue, 0) + 
            COALESCE(lr.total_interest_revenue, 0) + 
            COALESCE(sr.subscription_revenue, 0), 
            2
        ) AS total_customer_ltv
    FROM transaction_revenue tr
    LEFT JOIN loan_revenue lr ON tr.user_id = lr.user_id
    LEFT JOIN subscription_revenue sr ON tr.user_id = sr.user_id
)
SELECT 
    username,
    account_tier,
    registration_date,
    months_active,
    total_transactions,
    total_transaction_volume,
    transaction_fee_revenue,
    loan_interest_revenue,
    subscription_revenue,
    total_customer_ltv,
    ROUND(total_customer_ltv / NULLIF(months_active, 0), 2) AS avg_monthly_revenue,
    RANK() OVER (ORDER BY total_customer_ltv DESC) AS customer_value_rank
FROM customer_ltv
ORDER BY total_customer_ltv DESC;


-- ----------------------------------------------------------------------------
-- QUERY 4: Loan Default Prediction Analysis
-- Demonstrates: Risk Scoring, Statistical Metrics, Decision Logic
-- ----------------------------------------------------------------------------

WITH loan_metrics AS (
    SELECT 
        la.loan_id,
        la.user_id,
        u.username,
        la.loan_amount,
        la.interest_rate,
        la.loan_term_months,
        la.credit_score,
        la.monthly_income,
        la.employment_status,
        la.loan_status,
        la.loan_amount / (la.monthly_income * la.loan_term_months) AS dti_ratio,
        (la.loan_amount * (1 + la.interest_rate/100 * la.loan_term_months/12.0)) / la.loan_term_months AS monthly_payment,
        ((la.loan_amount * (1 + la.interest_rate/100 * la.loan_term_months/12.0)) / la.loan_term_months) / la.monthly_income AS payment_to_income_ratio,
        COUNT(t.transaction_id) AS transaction_count_last_90_days
    FROM loan_applications la
    JOIN users u ON la.user_id = u.user_id
    LEFT JOIN wallets w ON u.user_id = w.user_id
    LEFT JOIN transactions t ON w.wallet_id = t.sender_wallet_id 
        AND t.transaction_date >= la.application_date - INTERVAL '90 days'
    GROUP BY la.loan_id, la.user_id, u.username, la.loan_amount, la.interest_rate, 
             la.loan_term_months, la.credit_score, la.monthly_income, 
             la.employment_status, la.loan_status
),
risk_scoring AS (
    SELECT 
        *,
        (CASE 
            WHEN credit_score < 550 THEN 40
            WHEN credit_score < 600 THEN 30
            WHEN credit_score < 650 THEN 20
            WHEN credit_score < 700 THEN 10
            ELSE 0
        END) +
        (CASE 
            WHEN payment_to_income_ratio > 0.50 THEN 30
            WHEN payment_to_income_ratio > 0.40 THEN 20
            WHEN payment_to_income_ratio > 0.30 THEN 10
            ELSE 0
        END) +
        (CASE 
            WHEN employment_status = 'Unemployed' THEN 20
            WHEN employment_status = 'Self-Employed' THEN 10
            ELSE 0
        END) +
        (CASE 
            WHEN transaction_count_last_90_days < 5 THEN 10
            ELSE 0
        END) AS default_risk_score
    FROM loan_metrics
)
SELECT 
    loan_id,
    username,
    loan_amount,
    credit_score,
    ROUND(dti_ratio * 100, 2) AS dti_ratio_pct,
    ROUND(payment_to_income_ratio * 100, 2) AS payment_to_income_pct,
    employment_status,
    transaction_count_last_90_days,
    default_risk_score,
    CASE 
        WHEN default_risk_score >= 60 THEN 'High Risk - Reject'
        WHEN default_risk_score >= 40 THEN 'Medium Risk - Manual Review'
        WHEN default_risk_score >= 20 THEN 'Low Risk - Approve with Conditions'
        ELSE 'Low Risk - Auto Approve'
    END AS recommendation,
    loan_status
FROM risk_scoring
ORDER BY default_risk_score DESC;


-- ----------------------------------------------------------------------------
-- QUERY 5: Daily Active Users & Engagement Metrics
-- Demonstrates: Time Series Analysis, Growth Calculations, DAU/MAU
-- ----------------------------------------------------------------------------

WITH daily_active_users AS (
    SELECT 
        DATE(t.transaction_date) AS activity_date,
        COUNT(DISTINCT w.user_id) AS dau,
        COUNT(DISTINCT t.transaction_id) AS total_transactions,
        SUM(t.amount) AS total_volume
    FROM transactions t
    JOIN wallets w ON t.sender_wallet_id = w.wallet_id
    WHERE t.transaction_status = 'Completed'
    GROUP BY DATE(t.transaction_date)
),
monthly_active_users AS (
    SELECT 
        DATE_TRUNC('month', t.transaction_date) AS activity_month,
        COUNT(DISTINCT w.user_id) AS mau
    FROM transactions t
    JOIN wallets w ON t.sender_wallet_id = w.wallet_id
    WHERE t.transaction_status = 'Completed'
    GROUP BY DATE_TRUNC('month', t.transaction_date)
)
SELECT 
    dau.activity_date,
    dau.dau,
    dau.total_transactions,
    ROUND(dau.total_volume, 2) AS daily_volume,
    ROUND(dau.total_transactions::DECIMAL / dau.dau, 2) AS avg_transactions_per_user,
    ROUND(dau.total_volume / dau.dau, 2) AS avg_volume_per_user,
    mau.mau,
    ROUND((dau.dau::DECIMAL / mau.mau) * 100, 2) AS dau_mau_ratio,
    LAG(dau.dau, 7) OVER (ORDER BY dau.activity_date) AS dau_7_days_ago,
    ROUND(((dau.dau::DECIMAL - LAG(dau.dau, 7) OVER (ORDER BY dau.activity_date)) / 
           NULLIF(LAG(dau.dau, 7) OVER (ORDER BY dau.activity_date), 0)) * 100, 2) AS wow_growth_pct
FROM daily_active_users dau
LEFT JOIN monthly_active_users mau ON DATE_TRUNC('month', dau.activity_date) = mau.activity_month
ORDER BY dau.activity_date DESC;


-- ----------------------------------------------------------------------------
-- QUERY 6: Cross-Border Transaction Corridor Analysis
-- Demonstrates: International Payments, FX Revenue, Corridor Rankings
-- ----------------------------------------------------------------------------

WITH cross_border_transactions AS (
    SELECT 
        t.transaction_id,
        t.transaction_date,
        t.amount,
        t.currency,
        sw.user_id AS sender_user_id,
        su.country AS sender_country,
        rw.user_id AS receiver_user_id,
        ru.country AS receiver_country,
        CASE 
            WHEN su.country != ru.country THEN 'Cross-Border'
            ELSE 'Domestic'
        END AS transaction_category,
        CASE 
            WHEN su.country != ru.country THEN t.amount * 0.03
            ELSE 0
        END AS fx_revenue
    FROM transactions t
    JOIN wallets sw ON t.sender_wallet_id = sw.wallet_id
    JOIN users su ON sw.user_id = su.user_id
    JOIN wallets rw ON t.receiver_wallet_id = rw.wallet_id
    JOIN users ru ON rw.user_id = ru.user_id
    WHERE t.transaction_status = 'Completed'
),
corridor_analysis AS (
    SELECT 
        sender_country,
        receiver_country,
        COUNT(*) AS transaction_count,
        SUM(amount) AS total_volume,
        AVG(amount) AS avg_transaction_amount,
        SUM(fx_revenue) AS total_fx_revenue,
        MIN(transaction_date) AS first_transaction,
        MAX(transaction_date) AS last_transaction
    FROM cross_border_transactions
    WHERE transaction_category = 'Cross-Border'
    GROUP BY sender_country, receiver_country
)
SELECT 
    sender_country || ' → ' || receiver_country AS corridor,
    transaction_count,
    ROUND(total_volume, 2) AS total_volume,
    ROUND(avg_transaction_amount, 2) AS avg_transaction_amount,
    ROUND(total_fx_revenue, 2) AS fx_revenue,
    ROUND((total_fx_revenue / total_volume) * 100, 2) AS fx_revenue_rate_pct,
    first_transaction,
    last_transaction,
    RANK() OVER (ORDER BY total_volume DESC) AS corridor_rank
FROM corridor_analysis
ORDER BY total_volume DESC;


-- ============================================================================
-- SECTION 3: PERFORMANCE OPTIMIZATION
-- ============================================================================

CREATE INDEX idx_transactions_sender_date ON transactions(sender_wallet_id, transaction_date);
CREATE INDEX idx_transactions_status_date ON transactions(transaction_status, transaction_date);
CREATE INDEX idx_wallets_user_id ON wallets(user_id);
CREATE INDEX idx_loan_user_status ON loan_applications(user_id, loan_status);

-- ============================================================================
-- END OF FINTECH ANALYTICS PROJECT
-- ============================================================================
