## AXIS NPA QUERY

- There was grouping requirement on loan_ids with latest payout but other columns were also needed.

```
RAW SQL

select sub.loan_id, sub.dpd_days, sub.rank, sub.payout_date, sub.assigned_pos
from (select distinct fr.loan_id,
             fr.npa_flag,
             fr.transaction_id,
                      fr.assigned_pos,
             dense_rank() over (partition by icd.loan_account_no order by icd.payout_date desc) rank,
             icd.payout_date,
             fr.dpd_days,
             icd.investor_closing_pos,
             icd.investor_closing_principal_overdue,
             icd.investor_closing_interest_overdue,
             fmd.disbursement_date,
             fmd.disbursement_amount,
             fmd.asset_class,
             fmd.constitution_type,
             fmd.industry,
             fmd.restructured
      from fulfilment_report as fr
               INNER JOIN investor_collection_details icd on fr.loan_id = icd.loan_account_no
               INNER JOIN fulfilment_mcd_details as fmd on fmd.loan_number = icd.loan_account_no
      where fr.transaction_id in ('622b3817b7390b33d0d613bc', '623ee04b1e642c14f7c3b89a')
        and fr.npa_flag = 1) sub
where sub.rank = 1;
```

``` ruby
payout_wise_rank = InvestorReports::FulfilmentReport.where(transaction_id: transaction_ids, npa_flag: 1)
                                                    .joins("INNER JOIN fulfilment_mcd_details as mc ON mc.loan_number = fulfilment_report.loan_id INNER JOIN investor_collection_details as ic ON ic.loan_account_no = fulfilment_report.loan_id")
                                                    .select("fulfilment_report.transaction_id, DISTINCT ON (fulfilment_report.loan_id), fulfilment_report.dpd_days, fulfilment_report.security_amount, fulfilment_report.assigned_pos, mc.disbursement_date, mc.disbursement_amount, mc.asset_class,
                                                             mc.constitution_type, mc.industry, mc.restructured, ic.investor_closing_pos, ic.investor_closing_principal_overdue, ic.investor_closing_interest_overdue, fulfilment_report.assigned_pos as ap,
                                                             DENSE_RANK() OVER (PARTITION BY ic.loan_account_no ORDER BY ic.payout_date DESC as rank)")
                                                    .distinct # due to inner join with ic we will have duplicate records for loan_id, payout_date combination
                                                    .to_sql

InvestorReports::FulfilmentReport.from("(#{payout_wise_rank}) as t").where("t.rank = 1")
                                 .pluck(:transaction_id, :loan_id, :dpd_days, :security_amount, :disbursement_date,
                                        :disbursement_amount, :asset_class, :constitution_type, :industry, :restructured, :investor_closing_pos,
                                        :investor_closing_principal_overdue, :investor_closing_interest_overdue, :ap, :rank)
                                 .map { |d| @transaction_map[d[1]] = (@transaction_map[d[0]] || []) << d }
```                                             