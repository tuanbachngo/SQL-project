-- Auto-generated schema for vn_firm_panel
-- Model: DIM + FACT + SNAPSHOT (+ AUDIT) as per data dictionary
-- MySQL 8.0+ recommended (window functions used in vw_firm_panel_latest)

CREATE DATABASE IF NOT EXISTS `vn_firm_panel_test` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE `vn_firm_panel_test`;

SET FOREIGN_KEY_CHECKS=0;
DROP VIEW IF EXISTS `vw_firm_panel_latest`;
DROP TABLE IF EXISTS `fact_value_override_log`;
DROP TABLE IF EXISTS `fact_firm_year_meta`;
DROP TABLE IF EXISTS `fact_innovation_year`;
DROP TABLE IF EXISTS `fact_market_year`;
DROP TABLE IF EXISTS `fact_cashflow_year`;
DROP TABLE IF EXISTS `fact_financial_year`;
DROP TABLE IF EXISTS `fact_ownership_year`;
DROP TABLE IF EXISTS `fact_data_snapshot`;
DROP TABLE IF EXISTS `dim_firm`;
DROP TABLE IF EXISTS `dim_data_source`;
DROP TABLE IF EXISTS `dim_industry_l2`;
DROP TABLE IF EXISTS `dim_exchange`;
SET FOREIGN_KEY_CHECKS=1;

-- =========================
-- Tables
-- =========================

CREATE TABLE IF NOT EXISTS `dim_exchange` (
  `exchange_id` TINYINT AUTO_INCREMENT NOT NULL,
  `exchange_code` VARCHAR(10) NOT NULL,
  `exchange_name` VARCHAR(100) NULL,
  PRIMARY KEY (`exchange_id`),
  UNIQUE KEY `uq_dim_exchange_exchange_code` (`exchange_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `dim_industry_l2` (
  `industry_l2_id` SMALLINT AUTO_INCREMENT NOT NULL,
  `industry_l2_name` VARCHAR(150) NOT NULL,
  PRIMARY KEY (`industry_l2_id`),
  UNIQUE KEY `uq_dim_industry_l2_industry_l2_name` (`industry_l2_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `dim_data_source` (
  `source_id` SMALLINT AUTO_INCREMENT NOT NULL,
  `source_name` VARCHAR(100) NOT NULL,
  `source_type` ENUM('market','financial_statement','ownership','text_report','manual') NOT NULL,
  `provider` VARCHAR(150) NULL,
  `note` VARCHAR(255) NULL,
  PRIMARY KEY (`source_id`),
  UNIQUE KEY `uq_dim_data_source_source_name` (`source_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `dim_firm` (
  `firm_id` BIGINT AUTO_INCREMENT NOT NULL,
  `ticker` VARCHAR(20) NOT NULL,
  `company_name` VARCHAR(255) NOT NULL,
  `exchange_id` TINYINT NOT NULL,
  `industry_l2_id` SMALLINT NULL,
  `founded_year` SMALLINT NULL,
  `listed_year` SMALLINT NULL,
  `status` ENUM('active','delisted','inactive') NULL DEFAULT 'active',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`firm_id`),
  UNIQUE KEY `uq_dim_firm_ticker` (`ticker`),
  KEY `idx_dim_firm_exchange_id` (`exchange_id`),
  CONSTRAINT `fk_dim_firm_exchange_id` FOREIGN KEY (`exchange_id`)
    REFERENCES `dim_exchange` (`exchange_id`)
    ON DELETE RESTRICT ON UPDATE CASCADE,
  KEY `idx_dim_firm_industry_l2_id` (`industry_l2_id`),
  CONSTRAINT `fk_dim_firm_industry_l2_id` FOREIGN KEY (`industry_l2_id`)
    REFERENCES `dim_industry_l2` (`industry_l2_id`)
    ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `fact_data_snapshot` (
  `snapshot_id` BIGINT AUTO_INCREMENT NOT NULL,
  `snapshot_date` DATE NOT NULL,
  `period_from` DATE NULL,
  `period_to` DATE NULL,
  `fiscal_year` SMALLINT NOT NULL,
  `source_id` SMALLINT NOT NULL,
  `version_tag` VARCHAR(50) NULL,
  `created_by` VARCHAR(80) NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`snapshot_id`),
  UNIQUE KEY `uq_fact_data_snapshot_source_year_date_tag` (`source_id`,`fiscal_year`,`snapshot_date`,`version_tag`),
  KEY `idx_fact_data_snapshot_source_id` (`source_id`),
  CONSTRAINT `fk_fact_data_snapshot_source_id` FOREIGN KEY (`source_id`)
    REFERENCES `dim_data_source` (`source_id`)
    ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `fact_ownership_year` (
  `firm_id` BIGINT NOT NULL,
  `fiscal_year` SMALLINT NOT NULL,
  `snapshot_id` BIGINT NOT NULL,
  `managerial_inside_own` DECIMAL(10,6) NULL,
  `state_own` DECIMAL(10,6) NULL,
  `institutional_own` DECIMAL(10,6) NULL,
  `foreign_own` DECIMAL(10,6) NULL,
  `note` VARCHAR(255) NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`firm_id`,`fiscal_year`,`snapshot_id`),
  KEY `idx_fact_ownership_year_firm_id` (`firm_id`),
  CONSTRAINT `fk_fact_ownership_year_firm_id` FOREIGN KEY (`firm_id`)
    REFERENCES `dim_firm` (`firm_id`)
    ON DELETE RESTRICT ON UPDATE CASCADE,
  KEY `idx_fact_ownership_year_snapshot_id` (`snapshot_id`),
  CONSTRAINT `fk_fact_ownership_year_snapshot_id` FOREIGN KEY (`snapshot_id`)
    REFERENCES `fact_data_snapshot` (`snapshot_id`)
    ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `fact_financial_year` (
  `firm_id` BIGINT NOT NULL,
  `fiscal_year` SMALLINT NOT NULL,
  `snapshot_id` BIGINT NOT NULL,
  `unit_scale` BIGINT NOT NULL DEFAULT 1,
  `currency_code` CHAR(3) NOT NULL DEFAULT 'VND',
  `net_sales` DECIMAL(20,2) NULL,
  `total_assets` DECIMAL(20,2) NULL,
  `selling_expenses` DECIMAL(20,2) NULL,
  `general_admin_expenses` DECIMAL(20,2) NULL,
  `intangible_assets_net` DECIMAL(20,2) NULL,
  `manufacturing_overhead` DECIMAL(20,2) NULL,
  `net_operating_income` DECIMAL(20,2) NULL,
  `raw_material_consumption` DECIMAL(20,2) NULL,
  `merchandise_purchase_year` DECIMAL(20,2) NULL,
  `wip_goods_purchase` DECIMAL(20,2) NULL,
  `outside_manufacturing_expenses` DECIMAL(20,2) NULL,
  `production_cost` DECIMAL(20,2) NULL,
  `rnd_expenses` DECIMAL(20,2) NULL,
  `net_income` DECIMAL(20,2) NULL,
  `total_equity` DECIMAL(20,2) NULL,
  `total_liabilities` DECIMAL(20,2) NULL,
  `cash_and_equivalents` DECIMAL(20,2) NULL,
  `long_term_debt` DECIMAL(20,2) NULL,
  `current_assets` DECIMAL(20,2) NULL,
  `current_liabilities` DECIMAL(20,2) NULL,
  `growth_ratio` DECIMAL(10,6) NULL,
  `inventory` DECIMAL(20,2) NULL,
  `net_ppe` DECIMAL(20,2) NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`firm_id`,`fiscal_year`,`snapshot_id`),
  KEY `idx_fact_financial_year_firm_id` (`firm_id`),
  CONSTRAINT `fk_fact_financial_year_firm_id` FOREIGN KEY (`firm_id`)
    REFERENCES `dim_firm` (`firm_id`)
    ON DELETE RESTRICT ON UPDATE CASCADE,
  KEY `idx_fact_financial_year_snapshot_id` (`snapshot_id`),
  CONSTRAINT `fk_fact_financial_year_snapshot_id` FOREIGN KEY (`snapshot_id`)
    REFERENCES `fact_data_snapshot` (`snapshot_id`)
    ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `fact_cashflow_year` (
  `firm_id` BIGINT NOT NULL,
  `fiscal_year` SMALLINT NOT NULL,
  `snapshot_id` BIGINT NOT NULL,
  `unit_scale` BIGINT NOT NULL DEFAULT 1,
  `currency_code` CHAR(3) NOT NULL DEFAULT 'VND',
  `net_cfo` DECIMAL(20,2) NULL,
  `capex` DECIMAL(20,2) NULL,
  `net_cfi` DECIMAL(20,2) NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`firm_id`,`fiscal_year`,`snapshot_id`),
  KEY `idx_fact_cashflow_year_firm_id` (`firm_id`),
  CONSTRAINT `fk_fact_cashflow_year_firm_id` FOREIGN KEY (`firm_id`)
    REFERENCES `dim_firm` (`firm_id`)
    ON DELETE RESTRICT ON UPDATE CASCADE,
  KEY `idx_fact_cashflow_year_snapshot_id` (`snapshot_id`),
  CONSTRAINT `fk_fact_cashflow_year_snapshot_id` FOREIGN KEY (`snapshot_id`)
    REFERENCES `fact_data_snapshot` (`snapshot_id`)
    ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `fact_market_year` (
  `firm_id` BIGINT NOT NULL,
  `fiscal_year` SMALLINT NOT NULL,
  `snapshot_id` BIGINT NOT NULL,
  `shares_outstanding` BIGINT NULL,
  `price_reference` ENUM('close_year_end','avg_year','close_fiscal_year_end','manual') NULL,
  `share_price` DECIMAL(20,4) NULL,
  `market_value_equity` DECIMAL(20,2) NULL,
  `dividend_cash_paid` DECIMAL(20,2) NULL,
  `eps_basic` DECIMAL(20,6) NULL,
  `currency_code` CHAR(3) NOT NULL DEFAULT 'VND',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`firm_id`,`fiscal_year`,`snapshot_id`),
  KEY `idx_fact_market_year_firm_id` (`firm_id`),
  CONSTRAINT `fk_fact_market_year_firm_id` FOREIGN KEY (`firm_id`)
    REFERENCES `dim_firm` (`firm_id`)
    ON DELETE RESTRICT ON UPDATE CASCADE,
  KEY `idx_fact_market_year_snapshot_id` (`snapshot_id`),
  CONSTRAINT `fk_fact_market_year_snapshot_id` FOREIGN KEY (`snapshot_id`)
    REFERENCES `fact_data_snapshot` (`snapshot_id`)
    ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `fact_innovation_year` (
  `firm_id` BIGINT NOT NULL,
  `fiscal_year` SMALLINT NOT NULL,
  `snapshot_id` BIGINT NOT NULL,
  `product_innovation` TINYINT NULL,
  `process_innovation` TINYINT NULL,
  `evidence_source_id` SMALLINT NULL,
  `evidence_note` VARCHAR(500) NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`firm_id`,`fiscal_year`,`snapshot_id`),
  KEY `idx_fact_innovation_year_firm_id` (`firm_id`),
  CONSTRAINT `fk_fact_innovation_year_firm_id` FOREIGN KEY (`firm_id`)
    REFERENCES `dim_firm` (`firm_id`)
    ON DELETE RESTRICT ON UPDATE CASCADE,
  KEY `idx_fact_innovation_year_snapshot_id` (`snapshot_id`),
  CONSTRAINT `fk_fact_innovation_year_snapshot_id` FOREIGN KEY (`snapshot_id`)
    REFERENCES `fact_data_snapshot` (`snapshot_id`)
    ON DELETE RESTRICT ON UPDATE CASCADE,
  KEY `idx_fact_innovation_year_evidence_source_id` (`evidence_source_id`),
  CONSTRAINT `fk_fact_innovation_year_evidence_source_id` FOREIGN KEY (`evidence_source_id`)
    REFERENCES `dim_data_source` (`source_id`)
    ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `fact_firm_year_meta` (
  `firm_id` BIGINT NOT NULL,
  `fiscal_year` SMALLINT NOT NULL,
  `snapshot_id` BIGINT NOT NULL,
  `employees_count` INT NULL,
  `firm_age` SMALLINT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`firm_id`,`fiscal_year`,`snapshot_id`),
  KEY `idx_fact_firm_year_meta_firm_id` (`firm_id`),
  CONSTRAINT `fk_fact_firm_year_meta_firm_id` FOREIGN KEY (`firm_id`)
    REFERENCES `dim_firm` (`firm_id`)
    ON DELETE RESTRICT ON UPDATE CASCADE,
  KEY `idx_fact_firm_year_meta_snapshot_id` (`snapshot_id`),
  CONSTRAINT `fk_fact_firm_year_meta_snapshot_id` FOREIGN KEY (`snapshot_id`)
    REFERENCES `fact_data_snapshot` (`snapshot_id`)
    ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `fact_value_override_log` (
  `override_id` BIGINT AUTO_INCREMENT NOT NULL,
  `firm_id` BIGINT NOT NULL,
  `fiscal_year` SMALLINT NOT NULL,
  `table_name` VARCHAR(80) NOT NULL,
  `column_name` VARCHAR(80) NOT NULL,
  `old_value` VARCHAR(255) NULL,
  `new_value` VARCHAR(255) NULL,
  `reason` VARCHAR(255) NULL,
  `changed_by` VARCHAR(80) NULL,
  `changed_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`override_id`),
  KEY `idx_fact_value_override_log_firm_id` (`firm_id`),
  CONSTRAINT `fk_fact_value_override_log_firm_id` FOREIGN KEY (`firm_id`)
    REFERENCES `dim_firm` (`firm_id`)
    ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =========================
-- View: latest firm-year panel (firm-year + 39 variables)
-- =========================

CREATE OR REPLACE VIEW `vw_firm_panel_latest` AS
WITH k AS (
  SELECT DISTINCT firm_id, fiscal_year FROM `fact_financial_year`
  UNION
  SELECT DISTINCT firm_id, fiscal_year FROM `fact_ownership_year`
  UNION
  SELECT DISTINCT firm_id, fiscal_year FROM `fact_cashflow_year`
  UNION
  SELECT DISTINCT firm_id, fiscal_year FROM `fact_market_year`
  UNION
  SELECT DISTINCT firm_id, fiscal_year FROM `fact_innovation_year`
  UNION
  SELECT DISTINCT firm_id, fiscal_year FROM `fact_firm_year_meta`
),
oy_latest AS (
  SELECT oy.`firm_id`, oy.`fiscal_year`, oy.`snapshot_id`,
         oy.`managerial_inside_own`, oy.`state_own`, oy.`institutional_own`, oy.`foreign_own`
  FROM (
    SELECT oy.*, s.snapshot_date,
           ROW_NUMBER() OVER (PARTITION BY oy.firm_id, oy.fiscal_year ORDER BY s.snapshot_date DESC, oy.snapshot_id DESC) AS rn
    FROM `fact_ownership_year` oy
    JOIN `fact_data_snapshot` s ON s.snapshot_id = oy.snapshot_id
  ) oy
  WHERE oy.rn = 1
),
fy_latest AS (
  SELECT fy.`firm_id`, fy.`fiscal_year`, fy.`snapshot_id`,
         fy.`net_sales`, fy.`total_assets`, fy.`selling_expenses`, fy.`general_admin_expenses`,
         fy.`intangible_assets_net`, fy.`manufacturing_overhead`, fy.`net_operating_income`,
         fy.`raw_material_consumption`, fy.`merchandise_purchase_year`, fy.`wip_goods_purchase`,
         fy.`outside_manufacturing_expenses`, fy.`production_cost`, fy.`rnd_expenses`,
         fy.`net_income`, fy.`total_equity`, fy.`total_liabilities`,
         fy.`cash_and_equivalents`, fy.`long_term_debt`, fy.`current_assets`, fy.`current_liabilities`,
         fy.`growth_ratio`, fy.`inventory`, fy.`net_ppe`
  FROM (
    SELECT fy.*, s.snapshot_date,
           ROW_NUMBER() OVER (PARTITION BY fy.firm_id, fy.fiscal_year ORDER BY s.snapshot_date DESC, fy.snapshot_id DESC) AS rn
    FROM `fact_financial_year` fy
    JOIN `fact_data_snapshot` s ON s.snapshot_id = fy.snapshot_id
  ) fy
  WHERE fy.rn = 1
),
cf_latest AS (
  SELECT cf.`firm_id`, cf.`fiscal_year`, cf.`snapshot_id`,
         cf.`net_cfo`, cf.`capex`, cf.`net_cfi`
  FROM (
    SELECT cf.*, s.snapshot_date,
           ROW_NUMBER() OVER (PARTITION BY cf.firm_id, cf.fiscal_year ORDER BY s.snapshot_date DESC, cf.snapshot_id DESC) AS rn
    FROM `fact_cashflow_year` cf
    JOIN `fact_data_snapshot` s ON s.snapshot_id = cf.snapshot_id
  ) cf
  WHERE cf.rn = 1
),
my_latest AS (
  SELECT my.`firm_id`, my.`fiscal_year`, my.`snapshot_id`,
         my.`shares_outstanding`, my.`market_value_equity`, my.`dividend_cash_paid`, my.`eps_basic`
  FROM (
    SELECT my.*, s.snapshot_date,
           ROW_NUMBER() OVER (PARTITION BY my.firm_id, my.fiscal_year ORDER BY s.snapshot_date DESC, my.snapshot_id DESC) AS rn
    FROM `fact_market_year` my
    JOIN `fact_data_snapshot` s ON s.snapshot_id = my.snapshot_id
  ) my
  WHERE my.rn = 1
),
iv_latest AS (
  SELECT iv.`firm_id`, iv.`fiscal_year`, iv.`snapshot_id`,
         iv.`product_innovation`, iv.`process_innovation`
  FROM (
    SELECT iv.*, s.snapshot_date,
           ROW_NUMBER() OVER (PARTITION BY iv.firm_id, iv.fiscal_year ORDER BY s.snapshot_date DESC, iv.snapshot_id DESC) AS rn
    FROM `fact_innovation_year` iv
    JOIN `fact_data_snapshot` s ON s.snapshot_id = iv.snapshot_id
  ) iv
  WHERE iv.rn = 1
),
meta_latest AS (
  SELECT meta.`firm_id`, meta.`fiscal_year`, meta.`snapshot_id`,
         meta.`employees_count`, meta.`firm_age`
  FROM (
    SELECT meta.*, s.snapshot_date,
           ROW_NUMBER() OVER (PARTITION BY meta.firm_id, meta.fiscal_year ORDER BY s.snapshot_date DESC, meta.snapshot_id DESC) AS rn
    FROM `fact_firm_year_meta` meta
    JOIN `fact_data_snapshot` s ON s.snapshot_id = meta.snapshot_id
  ) meta
  WHERE meta.rn = 1
)
SELECT
  f.firm_id AS firm_id,
  f.ticker AS ticker,
  k.fiscal_year AS fiscal_year,

  -- (1)-(4) Ownership
  oy.managerial_inside_own AS managerial_inside_own,
  oy.state_own AS state_own,
  oy.institutional_own AS institutional_own,
  oy.foreign_own AS foreign_own,

  -- (5) Market shares
  my.shares_outstanding AS shares_outstanding,

  -- (6)-(18) Financial block
  fy.net_sales AS net_sales,
  fy.total_assets AS total_assets,
  fy.selling_expenses AS selling_expenses,
  fy.general_admin_expenses AS general_admin_expenses,
  fy.intangible_assets_net AS intangible_assets_net,
  fy.manufacturing_overhead AS manufacturing_overhead,
  fy.net_operating_income AS net_operating_income,
  fy.raw_material_consumption AS raw_material_consumption,
  fy.merchandise_purchase_year AS merchandise_purchase_year,
  fy.wip_goods_purchase AS wip_goods_purchase,
  fy.outside_manufacturing_expenses AS outside_manufacturing_expenses,
  fy.production_cost AS production_cost,
  fy.rnd_expenses AS rnd_expenses,

  -- (19)-(20) Innovation dummies
  iv.product_innovation AS product_innovation,
  iv.process_innovation AS process_innovation,

  -- (21)-(24)
  fy.net_income AS net_income,
  fy.total_equity AS total_equity,
  my.market_value_equity AS market_value_equity,
  fy.total_liabilities AS total_liabilities,

  -- (25)-(27) Cashflow
  cf.net_cfo AS net_cfo,
  cf.capex AS capex,
  cf.net_cfi AS net_cfi,

  -- (28)-(33)
  fy.cash_and_equivalents AS cash_and_equivalents,
  fy.long_term_debt AS long_term_debt,
  fy.current_assets AS current_assets,
  fy.current_liabilities AS current_liabilities,
  fy.growth_ratio AS growth_ratio,
  fy.inventory AS inventory,

  -- (34)-(35)
  my.dividend_cash_paid AS dividend_cash_paid,
  my.eps_basic AS eps_basic,

  -- (36)-(38)
  meta.employees_count AS employees_count,
  fy.net_ppe AS net_ppe,
  meta.firm_age AS firm_age

FROM k
JOIN `dim_firm` f ON f.firm_id = k.firm_id
LEFT JOIN oy_latest oy ON oy.firm_id=k.firm_id AND oy.fiscal_year=k.fiscal_year
LEFT JOIN fy_latest fy ON fy.firm_id=k.firm_id AND fy.fiscal_year=k.fiscal_year
LEFT JOIN cf_latest cf ON cf.firm_id=k.firm_id AND cf.fiscal_year=k.fiscal_year
LEFT JOIN my_latest my ON my.firm_id=k.firm_id AND my.fiscal_year=k.fiscal_year
LEFT JOIN iv_latest iv ON iv.firm_id=k.firm_id AND iv.fiscal_year=k.fiscal_year
LEFT JOIN meta_latest meta ON meta.firm_id=k.firm_id AND meta.fiscal_year=k.fiscal_year;

