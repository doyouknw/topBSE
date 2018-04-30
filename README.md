##Daily Top BSE

[Source](https://www.bseindia.com/markets/equity/EQReports/BhavCopyDebt.aspx?expandable=3)


#Python & CherryPY app
- Downloads the Equity bhavcopy zip from the above page
- Extracts and parses the CSV file in it
- Writes the records into Redis into appropriate data structures (Fields: code, name, open, high, low, close)
- Renders an HTML5 + CSS3 page that lists the top 10 stock entries from the Redis DB in a table
- Has a searchbox that lets you search the entries by the 'name' field in Redis and renders it in a table