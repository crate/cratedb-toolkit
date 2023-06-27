# cratedb-retentions backlog 

- Recurrent queries via scheduling. Yes, or no?

  ```
  @dag(
      start_date=dt.datetime.strptime("2021-11-19"),
      schedule="@daily",
      catchup=False,
  )
  """
  ```
