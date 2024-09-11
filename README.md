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

3. Using the data in “page_view_events”, what are the 5 most common paths that people follow in the app?

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

- Ofcourse, the second approach would be to look at pairs of paths, such as going from `/product-disclosures` to `/input-email` or going from `/input-email` to `/input-password` and so on. This approach would give us a better insight on how many people actually went on from one path to another, and getting the top 5 most visited paths out of this would tell us what are some of the most common steps people tend to complete when applying for a credit card and what are some of the steps (if we look at the rest of the result) that people do not reach or do not complete, where this relates a bit to `conversion`, as to how many people actually convert from one step to another and what steps do we still need to work on or follow up on the user to remind them to complete the remaining steps.

