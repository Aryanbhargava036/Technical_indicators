
-- 10 Day moving average
with Dailytable10 as(
	select *
	,Row_number() over(partition by Symbol order by `ï»¿Date` asc) as rownumber,
	Avg(Close) over(partition by Symbol order by `ï»¿Date` asc ROWS 9 PRECEDING) as DMA
from nifty.nifty50)
	
	select `ï»¿Date`, Symbol,Close,
	case when rownumber >9 then DMA
		 else  0 end as MA10
	
	from Dailytable10;
-- 5 Day moving average
with Dailytable5 as(
	select * 
	,Row_number() over(partition by Symbol order by `ï»¿Date` asc) as rownumber,
	Avg(Close) over(partition by Symbol order by `ï»¿Date` asc ROWS 4 PRECEDING) as DMA5
from nifty.nifty50)

	select `ï»¿Date`, Symbol,close,
	case when rownumber >4 then DMA5
		 else  0 end as MA5
	from Dailytable5;
-- 20 Day moving average
with Dailytable20 as(
	select *
	,Row_number() over(partition by Symbol order by `ï»¿Date` asc) as rownumber,
	Avg(Close) over(partition by Symbol order by `ï»¿Date` asc ROWS 19 PRECEDING) as DMA
from nifty.nifty50)

	select `ï»¿Date`, Symbol,close,
	case when rownumber >19 then DMA
	else  0 end as MA20
	from Dailytable20;
-- Volume
with Dailytable20 as(
	select *
	,Row_number() over(partition by Symbol order by `ï»¿Date` asc) as rownumber,
	Avg(Volume) over(partition by Symbol order by `ï»¿Date` asc ROWS 19 PRECEDING) as AVGVOLUME
from nifty.nifty50)
select `ï»¿Date`, Symbol,Close
	,case when rownumber>19 then AVGVOLUME 
	else 0 end as AVGVOLUME20
from Dailytable20;
-- RSI
with difference as (select `ï»¿Date`  as Date,Symbol as Symbol,Close as Close,
		Row_number() over(partition by Symbol order by `ï»¿Date` asc) as rownumber,
		Close - lag(Close,1) over(Partition by Symbol order by `ï»¿Date` asc) as difference
from nifty.nifty50),
gainloss as (select Date,Symbol,Close,rownumber,difference,
	case when difference >0  then difference
		when difference is Null then 0
		else 0  end as Gain,
	case when difference <=0  then -difference
		when difference is Null then 0
		else 0  end as Loss		
from difference),
avg_gainloss as(select gainloss.Date as Date, gainloss.Symbol as Symbol,
	gainloss.Close as Close,
	gainloss.rownumber as rownumbers,
	case when gainloss.rownumber >13 then 
		Avg(gainloss.Gain) over(partition by gainloss.Symbol
				order by gainloss.Date asc ROWS 14 PRECEDING)
				else 0 end as Avg_Gain
				,
		case when gainloss.rownumber >13 then 
		Avg(gainloss.loss) over(partition by gainloss.Symbol
				order by gainloss.Date asc ROWS 14 PRECEDING)
				else 0 end as Avg_Loss
from gainloss),

RS as (select avg_gainloss.rownumbers as rownumber,
	   avg_gainloss.Date as Date,avg_gainloss.Symbol as Symbol,
	   avg_gainloss.Close as Close,
 avg_gainloss.Avg_Gain as Avg_Gain,avg_gainloss.Avg_Loss as Avg_Loss,
case when avg_gainloss.Avg_Loss = 0 then 0
when avg_gainloss.rownumbers >=14 then avg_gainloss.Avg_Gain/avg_gainloss.Avg_Loss
			else 0 end as RS
from avg_gainloss)
select rs.Date,rs.Symbol,
	case when rs.rownumber>=14 then
		100-(100/(1+rs.RS))
		else 0 end as RSI
from RS;

