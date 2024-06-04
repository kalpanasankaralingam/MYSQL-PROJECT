
-- EXPLORATORY DATA ANALYSIS --
select max(total_laid_off),MAX(percentage_laid_off)
from layoff_staging2;

-- Which are the companies did 100% lay off--

SELECT * FROM LAYOFF_STAGING2
WHERE percentage_laid_off = 1
order by total_laid_off desc;

SELECT * FROM LAYOFF_STAGING2
WHERE percentage_laid_off = 1
order by funds_raised_millions desc;

--which are the companies did maximum layoff--

select company, sum(total_laid_off)
from layoff_staging2 group by company order by 2 desc;

select min(`date`), max(`date`)
from layoff_staging2;

-- In whch industry we have maximum employee layoff--

select industry, sum(total_laid_off)
from layoff_staging2 group by industry order by 2 desc;


select country, sum(total_laid_off)
from layoff_staging2 group by country order by 2 desc;

select year(`date`), sum(total_laid_off)
from layoff_staging2 group by year(`date`) order by 1 desc;

select stage, sum(total_laid_off)
from layoff_staging2 group by stage order by 2 desc;

with ROLLING_TOTAL AS(
select substring(`date`,1,7) as `month`, sum(total_laid_off) AS TOTAL_OFF
from  layoff_staging2 where substring(`date`,1,7) is not null  group by `month` order by 1 asc)

SELECT `month`,TOTAL_OFF,SUM(TOTAL_OFF) OVER(ORDER BY `month`) as rolling_total
from ROLLING_TOTAL;

select company, year(`date`) ,sum(total_laid_off)
from layoff_staging2 group by company, year(`date`)
order by company asc;

select company, year(`date`) ,sum(total_laid_off)
from layoff_staging2 group by company, year(`date`)
order by 3 desc;

with company_year(company, years, total_laid_off) as
(
select company, year(`date`) ,sum(total_laid_off)
from layoff_staging2 group by company, year(`date`)
), company_year_rank as
(
select *,dense_rank() over(partition by years order by total_laid_off desc) as ranking from company_year
where years is not null)

select * from company_year_rank
where ranking <= 5;
