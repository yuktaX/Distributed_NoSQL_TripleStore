--DROP Table if exists sample_yago;
 
CREATE TABLE sample_yago (
    subject TEXT,
    predicate TEXT,
    object TEXT
);

CREATE TABLE current_seq (
    seq_no bigint
);

insert into current_seq values(1);

--load text.csv into sample_yago
