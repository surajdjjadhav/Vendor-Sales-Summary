

-- for running this file in bash run thi cmd
-- sqlite3 database/vendor_performance.db < database/database.sql


.headers on
.mode column



-- DELETE statements here


SELECT *
FROM (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY InventoryId, Store, Brand, Description, Size, SalesQuantity,
                        SalesDollars, SalesPrice, SalesDate, Volume, Classification,
                        ExciseTax, VendorNo, VendorName
           ORDER BY rowid
         ) AS rank_
  FROM sales
)
WHERE rank_ > 1;


BEGIN TRANSACTION;

CREATE INDEX IF NOT EXISTS idx_sales_partition ON sales (
  InventoryId, Store, Brand, Description, Size, SalesQuantity,
  SalesDollars, SalesPrice, SalesDate, Volume, Classification,
  ExciseTax, VendorNo, VendorName
);

CREATE INDEX IF NOT EXISTS idx_purchase_prices_partition ON purchase_prices (
  Brand, Description, Price, Size, Volume,
  Classification, PurchasePrice, VendorNumber, VendorName
);

CREATE INDEX IF NOT EXISTS idx_purchases_partition ON purchases (
  InventoryId, Store, Brand, Description, Size,
  VendorNumber, VendorName, PONumber, PODate,
  ReceivingDate, InvoiceDate, PayDate, PurchasePrice,
  Quantity, Dollars, Classification
);


CREATE INDEX IF NOT EXISTS idx_end_inventory_partition ON end_inventory (
  InventoryId, Store, City, Brand, Description, Size, onHand, Price, endDate
);


CREATE INDEX IF NOT EXISTS idx_begin_inventory_partition ON begin_inventory (
  InventoryId, Store, City, Brand, Description, Size, onHand, Price, startDate
);
CREATE INDEX IF NOT EXISTS idx_vendor_invoice_partition ON vendor_invoice (
  VendorNumber, VendorName, InvoiceDate, PONumber, PODate,
  PayDate, Quantity, Dollars, Freight, Approval
);


 ---   deletind dupliccates from sales table -------------
DELETE FROM sales
WHERE rowid IN (
  SELECT rowid
  FROM (
    SELECT rowid,
           ROW_NUMBER() OVER (
             PARTITION BY InventoryId, Store, Brand, Description, Size, SalesQuantity,
                          SalesDollars, SalesPrice, SalesDate, Volume, Classification,
                          ExciseTax, VendorNo, VendorName
             ORDER BY rowid
           ) AS rank_
    FROM sales
  )
  WHERE rank_ > 1
);

-------- deleting dduplicates form purchase price table-----------
DELETE FROM purchase_prices
WHERE rowid IN (
  SELECT rowid
  FROM (
    SELECT rowid,
           ROW_NUMBER() OVER (
             PARTITION BY Brand, Description, Price, Size, Volume,
                          Classification, PurchasePrice, VendorNumber, VendorName
             ORDER BY rowid
           ) AS rank_
    FROM purchase_prices
  )
  WHERE rank_ > 1
);


-- Deleting duplicates from the purchases table
DELETE FROM purchases
WHERE rowid IN (
  SELECT rowid
  FROM (
    SELECT rowid,
           ROW_NUMBER() OVER (
             PARTITION BY InventoryId, Store, Brand, Description, Size,
                          VendorNumber, VendorName, PONumber, PODate,
                          ReceivingDate, InvoiceDate, PayDate, PurchasePrice,
                          Quantity, Dollars, Classification
             ORDER BY rowid
           ) AS rank_
    FROM purchases
  )
  WHERE rank_ > 1
);


-- Deleting duplicates from the end_inventory

DELETE FROM end_inventory
WHERE rowid IN (
  SELECT rowid FROM (
    SELECT rowid,
           ROW_NUMBER() OVER (
             PARTITION BY InventoryId, Store, City, Brand, Description, Size, onHand, Price, endDate
             ORDER BY rowid
           ) AS rank_
    FROM end_inventory
  )
  WHERE rank_ > 1
);


-- Deleting duplicates from the  begin_inventory
DELETE FROM begin_inventory
WHERE rowid IN (
  SELECT rowid FROM (
    SELECT rowid,
           ROW_NUMBER() OVER (
             PARTITION BY InventoryId, Store, City, Brand, Description, Size, onHand, Price, startDate
             ORDER BY rowid
           ) AS rank_
    FROM begin_inventory
  )
  WHERE rank_ > 1
);


-- Deleting duplicates from the  vendor_invoice
DELETE FROM vendor_invoice
WHERE rowid IN (
  SELECT rowid FROM (
    SELECT rowid,
           ROW_NUMBER() OVER (
             PARTITION BY VendorNumber, VendorName, InvoiceDate, PONumber, PODate,
                          PayDate, Quantity, Dollars, Freight, Approval
             ORDER BY rowid
           ) AS rank_
    FROM vendor_invoice
  )
  WHERE rank_ > 1
);


-- Show remaining duplicates in vendor_invoice
SELECT COUNT(*) AS remaining_duplicates
FROM (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY VendorNumber, VendorName, InvoiceDate, PONumber, PODate,
                        PayDate, Quantity, Dollars, Freight, Approval
           ORDER BY rowid
         ) AS rank_
  FROM vendor_invoice
)
WHERE rank_ > 1;


COMMIT;





