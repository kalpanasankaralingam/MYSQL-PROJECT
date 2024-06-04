select * from layoffs;
-- Creating a copy of original table to work on it--

create table layoff_staging
like layoffs;

insert into layoff_staging
select * from layoffs;
-- step1: Removing Duplicate Values--
-- There is no id in this data, so we have delete duplicate rows by creating row number--
WITH DUPLICATE_CTE AS(
		select *, ROW_NUMBER() OVER(
		partition by company,location,industry,total_laid_off, percentage_laid_off, date,
        stage, country,funds_raised_millions) AS ROW_NUM
		 from layoff_staging)

SELECT * FROM DUPLICATE_CTE WHERE ROW_NUM >1;
-- we cannot delete/ update cte, so create a new table along with row number--

create TABLE `layoff_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `ROW_NUM` int        -- This is the column created in duplicate cte--
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoff_staging2;

insert into layoff_staging2
select *, ROW_NUMBER() OVER(
		partition by company,location,industry,total_laid_off, percentage_laid_off, date,
        stage, country,funds_raised_millions) AS ROW_NUM
		 from layoff_staging;
         
select * from layoff_staging2 where ROW_NUM > 1;
delete from  layoff_staging2 where ROW_NUM > 1;

-- Trimming of extra spaces--

select* from layoff_staging2;
select company, trim(company) from layoff_staging2;


-- step2: standardization of data--
update layoff_staging2
set company = trim(company);

select distinct industry from layoff_staging2 order by 1;
 select * from layoff_staging2 where industry like 'crypto%';
 
 update layoff_staging2 
 set industry = "crypto" where industry like 'crypto%';
 
 select distinct country from layoff_staging2 order by 1;
 update layoff_staging2 
 set country = trim(TRAILING '.' from country);
 
 select `date`, str_to_date(`date`, '%m/%d/%Y') from layoff_staging2;
 
 update layoff_staging2
 set `date` = str_to_date(`date`, '%m/%d/%Y');
 
 alter table layoff_staging2
 modify column `date` date;
 
 -- step3: NULL OR BLANK VALUES--
 select * from layoff_staging2
 where industry is null or industry = '';
 
 select * from layoff_staging where company = 'Airbnb';
 
 select * from layoff_staging2
 where company = 'Airbnb';
 
 update layoff_staging2
 set industry = null where industry = '';
 
 select * from layoff_staging2 t1
 join layoff_staging t2
 on t1.company = t2.company and t1.location = t2.location
 where t1.industry is null and t2.industry is not null;
 
 
 update layoff_staging2 t1
 join layoff_staging2 t2
 on t1.company = t2.company
 set t1.industry = t2.industry
 where t1.industry is null and
 t2.industry is not null;
 
 select * from layoff_staging2; 
 
 -- Step4: REMOVE UNNECESSARY COLUMNS--
 
 delete from layoff_staging2 where
 total_laid_off is null
 and percentage_laid_off is null;
 
 alter table layoff_staging2 drop column ROW_NUM;
 select * from layoff_staging2;
