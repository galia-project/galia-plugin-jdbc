# From Cantaloupe 5.0.6

1. The `JdbcCache.derivative_image_table` configuration key has been renamed
   to `JDBCCache.variant_image_table`.
2. Add and implement the `jdbcsource_last_modified()` method from the sample 
   delegate script.
3. If you are using a Java delegate, you must implement the new `invoke()`
   method in such a way that it can invoke any of the four delegate methods
   (including the new `jdbcsource_last_modified()`).
4. If you are using JDBCCache, change the `DATETIME`-type columns to
   `TIMESTAMP`.
5. If you are using JDBCCache, add a `TIMESTAMP`-type `last_modified` column
   to the image and info tables.
