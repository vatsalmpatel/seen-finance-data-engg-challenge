# Seen Data Engineering SQL Challenge

### 1. Write a query to determine the total balances each customer owes at the end of each day. Output columns should include: customer_id , snapshot_date , customer_balance

- Approach 1 is just a pure customer_balance at the end of each day. 

```SQL
with query as(
	select acct.customer_id,
	DATE(acct.transaction_date) as snapshot_date,
	SUM(acct.transaction_amount) as customer_balance
	from accounts_transactions acct
	group by acct.customer_id, DATE(acct.transaction_date)
)
select customer_id, snapshot_date, customer_balance from query order by customer_id, snapshot_date;
```

- Approach 2, this maintains a sort of running total for the customer balance at the of each day. This means it also includes the balance from the previous day in the customer_balance for each column.

```SQL
with query as(
	select 
	acct.customer_id,
	DATE(acct.transaction_date) as snapshot_date,
	SUM(acct.transaction_amount) over(partition by acct.customer_id order by DATE(acct.transaction_date) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as customer_balance
	from accounts_transactions acct
)
select customer_id, snapshot_date, customer_balance from query order by customer_id, snapshot_date;
```

- Approach 3, this zeros out the running total at the end of each month, assuming timely payments:

```SQL
with query as(
	select acct.customer_id,
    DATE(acct.transaction_date) as snapshot_date,
    SUM(acct.transaction_amount) over (PARTITION BY acct.customer_id, strftime('%Y-%m', acct.transaction_date) order by DATE(acct.transaction_date) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as customer_balance
	from accounts_transactions acct
)
select customer_id, snapshot_date, customer_balance from query order by customer_id, snapshot_date;
```

### 2. Write a query that lists unusual transactions. It’s up to you to define what unusual means. You might want to use some statistical functions.

- As the definition of unusual transactions is left undefined, I defined unusual transaction as those transaction, which are atleast three standard deviations away from the mean. Now, this was a bit long calculation, as SQLite does not provide any function to get the standard deviation, and when I tried calculating standard deviation and mean together, it gave me an error. So I had to calculate the mean (avegare) and the standard deviation in two different cte's, and then combine together in the third cte, to get all the transacitons that are three standard deviations away from the mean. I calculate a new column called `flagged_trans` and it is set to 1 if the transaction is atleast 3 standard deviations away from the mean (I had to google the formula for standard deviation). I am only showing transactions where `flagged_trans = 1`. Feel free to remove that condition and check the entire output.

```SQL
with avg_per_cust_calc as(
	select customer_id,
	AVG(transaction_amount) as avg_trans_amt
	from accounts_transactions
	group by customer_id
),
stddev_per_cust_calc as(
	select acct.customer_id,
	apcc.avg_trans_amt,
	SQRT((SUM((acct.transaction_amount - apcc.avg_trans_amt) * (acct.transaction_amount - apcc.avg_trans_amt))) / ((COUNT(*) - 1))) as stddev_amt
	from accounts_transactions acct
	INNER JOIN avg_per_cust_calc apcc on acct.customer_id = apcc.customer_id
	group by acct.customer_id
),
query as(
	select acct.customer_id,
	acct.account_id,
	acct.transaction_date,
	acct.transaction_amount,
	calc.avg_trans_amt,
	calc.stddev_amt,
	CASE WHEN (ABS(acct.transaction_amount - calc.avg_trans_amt)) > (calc.stddev_amt * 3) then 1 else 0 end as flagged_trans
	from accounts_transactions acct
	inner join stddev_per_cust_calc calc on acct.customer_id = calc.customer_id
)
--select * from avg_per_cust_calc;
--select * from stddev_per_cust_calc;
select * from query where flagged_trans = 1;
```

### 3. Using the data in “page_view_events”, what are the 5 most common paths that people follow in the app?

- Now, here I am thinking of two approaches, first approach is looking at just single paths such as `/input-email` or `/product-disclosures` and then use DENSE_RANK() to rank each path by its count in descending order and then return all the paths that have `rank <= 5`. But, this would be too simple of an approach, and too basic in a sense that it does not provide enough insight about the paths that people follow, it just gives us how many people landed on this particular path. But anyways, here is the SQL query of the first approach:

```SQL
with query as(
	select path, count(*) as total_visits,
	DENSE_RANK() OVER(order by count(*) desc) as rnk
	from page_view_events
	group by path
)
select * from query where rnk <= 5;
```

- Ofcourse, the second approach would be to look at pairs of paths, such as going from `/product-disclosures` to `/input-email` or going from `/input-email` to `/input-password` and so on. This approach would give us a better insight on how many people actually went on from one path to another, and getting the top 5 most visited paths out of this would tell us what are some of the most common steps people tend to complete when applying for a credit card and what are some of the steps (if we look at the rest of the result) that people do not reach or do not complete, where this relates a bit to `conversion`, as to how many people actually convert from one step to another and what steps do we still need to work on or follow up on the user to remind them to complete the remaining steps. Now, this could be extended to 3 paths, 4 paths and beyond, but I think that would be repition of code, with some minor changes, so I will restrict myself to 2 paths (pair of path such as `/product-disclosures` to `/input-email`) and then explain what is needed to be done to get three or more paths.

```SQL
with query1 as(
	select pve.visitor_id,
	pve.path,
	ROW_NUMBER() OVER(partition by pve.visitor_id order by pve.event_time) as rnk
	from page_view_events pve
),
query as(
	select a.path as p1,
	b.path as p2,
	count(*) as 'number_of_movements',
	DENSE_RANK() OVER(order by COUNT(*) DESC) as d_rnk
	from query1 a inner join query1 b on 
	a.visitor_id = b.visitor_id and a.rnk = b.rnk - 1
	group by 1,2
)
select CONCAT(p1," to ",p2) as "path",number_of_movements from query 
where d_rnk <= 5;
```

- After doubt clarification from Brendan, I was supposed to consider full-paths, not just dual paths, so here is the query that conssiders full uer path, rather than just considering dual paths. This give us much more clarification on what complete paths user follow.

```SQL
with complete_paths_cte as(
	select STRING_AGG(pve.path, " to " order by pve.event_time) as "complete_user_path",
	pve.visitor_id
	from page_view_events pve
	group by visitor_id
),
user_path_cnt as(
	select cpc.complete_user_path,
	COUNT(*) as total_path_visits,
	DENSE_RANK() OVER(order by count(*) desc) as rnk
	from
	complete_paths_cte cpc
	group by 1
)
select complete_user_path, total_path_visits
from user_path_cnt
where rnk <= 5;
```


### 4. Using the data in “page_view_events”, write a query that can be used to display the following funnel. The funnel above shows how many people entered a step, and the percentage which dropped off before the next step.

1. /product-disclosures
2. /input-email
3. /input-password
4. /verify-email
5. /input-phone
6. /input-sms
7. /kyc-info
8. /input-name
9. /input-address
10. /date-of-birth
11. /input-ssn
12. /input-income
13. /confirm-details

- My approach here was to first create a look-up table, as we already know that this is the path that users should follow, we can make this a separate look-up table. Query to create a look-up table is as follows, and you need to run this first, because it is going to be used in the final query:

```SQL
CREATE TABLE IF NOT EXISTS user_path (
	 path_number NUMBER,
	 path_name STRING
);

INSERT INTO user_path values
(1,"/product-disclosures"),
(2,"/input-email"),
(3,"/input-password"),
(4,"/verify-email"),
(5,'/input-phone'),
(6,"/input-sms"),
(7,"/kyc-info"),
(8,"/input-name"),
(9,"/input-address"),
(10,'/date-of-birth'),
(11,"/input-ssn"),
(12,"/input-income"),
(13,"/confirm-details");
```

- Now using this as the look-up table, we first give each visitor's visit to a specific path a number, ordered by the `event_time` in the CTE `each_visitor_path_seq`. This will help us ensure that the user follows the sequence of paths correctly. In the `count_visitor_by_correct_path` CTE, we count the number of visitors per path and make sure that the sequence of paths is followed by each user. And lastly, in the `drop_off_calc` CTE, we calculate the drop off percentage of number of visitors that dropped of before the next step. Naturally, the last path `/confirm-details` will have `NULL` as drop off percentage, because there are no other steps after this and we can't calculate how many visitors dropped off before going to the next step, and I replace that with `0`.

```SQL
with each_visitor_path_seq as(
	select up.path_number, up.path_name,
	pve.visitor_id,
	ROW_NUMBER() OVER(partition by pve.visitor_id order by pve.event_time) as step_number
	from
	page_view_events pve 
	inner join
	user_path up on up.path_name = pve.path
),
--select * from each_visitor_path_seq;
count_visitor_by_correct_path as(
	select evps.path_number, evps.path_name,
	COUNT(DISTINCT evps.visitor_id) as num_visitors
	from each_visitor_path_seq evps
	where step_number = path_number
	group by path_number
),
-- select * from count_visitor_by_correct_path
drop_off_calc as(
	select a.path_number, a.path_name,
	a.num_visitors,
	IFNULL(ROUND(((a.num_visitors - b.num_visitors) * 100.0 / (a.num_visitors)),3),0) as drop_off_perc
	from count_visitor_by_correct_path a
	left join count_visitor_by_correct_path b on a.path_number = b.path_number - 1
	order by a.path_number
)
select * from drop_off_calc;
```