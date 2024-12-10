-- Create schema
CREATE SCHEMA transformed;

-- Create table emp_req
drop table transformed.emp_req;

CREATE TABLE transformed.emp_req (
    From_Time VARCHAR(100) NOT NULL,
    To_Time VARCHAR(100) NOT NULL,
    Reg_Up_Needed INT NOT NULL,
    Reg_Down_Needed INT NOT NULL,
    Greeter_Up_Needed INT NOT NULL,
    Greeter_Down_Needed INT NOT NULL,
    Min_Total_Emp_Needed INT NOT NULL,
    Total_Avl_Emp FLOAT NOT NULL,
    Availability_Check_Flag BOOLEAN NOT NULL
);

select * from transformed.emp_req;

-- Create table work_status
drop table transformed.work_status;

CREATE TABLE transformed.work_status (
    Name VARCHAR(255) NOT NULL,
    Start_time VARCHAR(100) NOT NULL,
    End_time VARCHAR(100) NOT NULL,
    Working_Flag INT NOT NULL,
    Remaining_hours_left FLOAT NOT NULL
);

select * from transformed.work_status;