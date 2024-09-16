--Data cleaning

select *
from layoff_staging;

--created duplicated table 'layoff_staging' where we work on the data
create table layoff_staging
select *
from layoffs;

    -- Main steps for data clenaing
-- Removing Duplicates
-- Standardizing values
-- Remove null and blank values and alter the table


-- duplicate remove

-- create cte to find the duplicate values and add row_num column to find repeated data
with duplicate_cte as
(
select *,
row_number () over (partition by company, location, industry, total_laid_off, percentage_laid_off, 
`date`, stage, country, funds_raised_millions) as row_num
from layoff_staging

)

select *
from duplicate_cte
where company = 'casper';

select *
from layoff_staging;

-- create table 'layoff_staging2' to remove duplicates by using row_num column
CREATE TABLE `layoff_staging2` (
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

select *
from layoff_staging2;

-- inserting row_num values with whole data into 'layoff_staging2' table
insert into layoff_staging2
select *,
row_number () over (partition by company, location, industry, total_laid_off, percentage_laid_off, 
`date`, stage, country, funds_raised_millions) as row_num
from layoff_staging;

--find the data where row_num values greater than 1 due to which find common values 
select * 
from layoff_staging2
where row_num >1 ;

select * 
from layoff_staging2;

--deleting row_num values greter then 1
delete 
from layoff_staging2
where row_num >1 ;


-- Standardizing

select *
from layoff_staging2  ;

select distinct company
from layoff_staging2
order by 1; 

update layoff_staging2
set company = trim(company);


select distinct industry
from layoff_staging2
order by 1; 

update layoff_staging2
set  industry = null 
where industry = '';

SELECT distinct industry
FROM layoff_staging2
WHERE industry LIKE 'crypto%';

update layoff_staging2
set  industry = 'Crypto' 
where industry like 'Crypto%';

select distinct country
from layoff_staging2
order by 1;

update layoff_staging2
set country = 'United Sates'
where country like 'united states%';


select `date`
from layoff_staging2;

select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoff_staging2; 

update layoff_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

select *
from layoff_staging2;

alter table layoff_staging2
modify column `date` date;



-- REMOVE NULL AND BLANK AND ALTER TABLE


SELECT *
FROM layoff_staging2;

SELECT distinct industry
FROM layoff_staging2
ORDER BY 1;

SELECT *
FROM layoff_staging2
WHERE industry IS NULL 
ORDER BY industry;


UPDATE layoff_staging2 t1
JOIN layoff_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

DELETE 
FROM layoff_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

ALTER TABLE layoff_staging2
DROP COLUMN row_num;

SELECT *
FROM layoff_staging2;













