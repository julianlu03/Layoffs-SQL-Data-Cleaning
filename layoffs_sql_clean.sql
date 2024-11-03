SELECT *
FROM layoffs_raw;

-- Steps to data clean
-- 1. Get rid of duplicate rows
-- 2. Standardize the Data
-- 3. Fill in or get rid of null and blank values 
-- 4. Get rid of columns that are not useful

-- Make copy of the raw data first and work with the copy
CREATE TABLE copy_layoffs
LIKE layoffs_raw;

INSERT INTO copy_layoffs
SELECT * FROM layoffs_raw;

SELECT *
FROM copy_layoffs;

-- 1. Get rid of duplicate rows
-- Checking for duplicates via assigning row numbers
-- Necessary due to not having a unique row value in this dataset

SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM copy_layoffs;

-- Since we can't check the column row_num in a query after (because it is not a permanent column), create a CTE instead

WITH duplicate_finder AS 
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM copy_layoffs
)
SELECT * 
FROM duplicate_finder
WHERE row_num > 1;

-- Double check whether these rows are duplicates

SELECT *
FROM copy_layoffs
WHERE company = 'Yahoo';


-- Now to get rid of the duplicate companies
-- Create a new table where row_num is a permanent column

CREATE TABLE `copy_layoffs2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM copy_layoffs2;

INSERT INTO copy_layoffs2
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM copy_layoffs
);

SELECT *
FROM copy_layoffs2
WHERE row_num > 1;

DELETE FROM copy_layoffs2
WHERE row_num > 1;

-- Successfully deleted all duplicate rows

-- 2. Standardize the data

-- Trimming whitespace from company names
SELECT company, TRIM(company)
FROM copy_layoffs2;

UPDATE copy_layoffs2
SET company = TRIM(company);

-- Change date column to a more standard format
-- Originally had a syntax error because some `date` values had awkward formatting, had to trim

SELECT `date`, STR_TO_DATE(TRIM(`date`), '%m/%d/%Y')
FROM copy_layoffs2;

UPDATE copy_layoffs2
SET `date` = STR_TO_DATE(TRIM(`date`), '%m/%d/%Y');

SELECT *
FROM copy_layoffs2;

-- Now to change the `date` column datatype from text to date datatype

ALTER TABLE copy_layoffs2
MODIFY COLUMN `date` date;

-- Check whether the industry, location, or country names do not have different wording for same names

SELECT DISTINCT location
FROM copy_layoffs2
ORDER BY 1;


SELECT DISTINCT industry
FROM copy_layoffs2
ORDER BY 1;

-- 'Crypto', 'Crypto Currency', and 'CryptoCurrency' all mean the same thing but are represented as three distinct industries
-- Combine all into the industry called 'Crypto'

SELECT industry
FROM copy_layoffs2
WHERE industry LIKE 'Crypto%';

UPDATE copy_layoffs2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Now checking countries

SELECT DISTINCT country
FROM copy_layoffs2
ORDER BY 1;

-- Instance of 'United States' being represented as 'United States.'

SELECT DISTINCT country
FROM copy_layoffs2
WHERE country LIKE 'United States%';

UPDATE copy_layoffs2
SET country = 'United States'
WHERE country = 'United States.';

SELECT *
FROM copy_layoffs2;


-- 3. Fill in or get rid of null and blank values 

-- Airbnb, Carvana, and Juul all have blank industry values
-- Try to find other rows with the same company to fill in some empty values

SELECT *
FROM copy_layoffs2
WHERE company = 'Juul';

-- Selfjoin 
-- Duplicate companies will match with their same row and also a unique row with the same company names (different values)
-- The WHERE statement makes it so that rows with the same companies do not match up with the copy of the same row, only different ones

SELECT lf1.company, lf2.company, lf1.industry, lf2.industry
FROM copy_layoffs2 lf1
JOIN copy_layoffs2 lf2
	ON lf1.company = lf2.company
WHERE (lf1.industry IS NULL OR lf1.industry = '')
AND (lf2.industry IS NOT NULL AND lf2.industry <> '');


UPDATE copy_layoffs2 lf1
JOIN copy_layoffs2 lf2
	ON lf1.company = lf2.company
SET lf1.industry = lf2.industry
WHERE (lf1.industry IS NULL OR lf1.industry = '')
AND (lf2.industry IS NOT NULL AND lf2.industry <> '');


-- 4. Get rid of columns and rows that are not useful

-- Get rid of rows in which both total_laid_off and percentage_laid_off are NULL or '', because those rows won't be useful for the goal of this project regarding layoffs
DELETE FROM copy_layoffs2
WHERE (total_laid_off IS NULL OR total_laid_off = '')
AND (percentage_laid_off IS NULL OR percentage_laid_off = '');


-- No longer need the row_num column
ALTER TABLE copy_layoffs2
DROP COLUMN row_num;

SELECT *
FROM copy_layoffs2
WHERE (total_laid_off IS NULL OR total_laid_off = '')
AND (percentage_laid_off IS NULL OR percentage_laid_off = '');

SELECT *
FROM copy_layoffs2;