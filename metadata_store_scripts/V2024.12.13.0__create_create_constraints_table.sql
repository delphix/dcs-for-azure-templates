CREATE TABLE capture_constraints (
    dataset VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    specified_database VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    specified_schema VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    identified_parent_table VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    child_table VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    constraint_name VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    parent_columns VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    children_columns VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    before_masking_status VARCHAR(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    last_captured_timestamp DATETIME,
    after_masking_status VARCHAR(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    CONSTRAINT capture_constraints_pk PRIMARY KEY (
        dataset, 
        specified_database, 
        specified_schema, 
        identified_parent_table, 
        child_table, 
        constraint_name
    )
);