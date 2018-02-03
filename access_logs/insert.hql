
INSERT OVERWRITE TABLE logs.tokenized_access_logs 
SELECT * FROM logs.intermediate_access_logs;