CREATE TABLE public.items (
    id serial NOT NULL,
    name text NOT NULL,
    code text,
    CONSTRAINT items_pkey PRIMARY KEY (id)
);

CREATE TABLE public.parts (
    id serial NOT NULL,
    item_id integer,
    name text NOT NULL,
    CONSTRAINT parts_pkey PRIMARY KEY (id)
);

CREATE TABLE public.repair_log_tools (
    log_id integer NOT NULL,
    tool_id integer NOT NULL,
    is_damaged boolean DEFAULT false NOT NULL,
    note text,
    CONSTRAINT repair_log_tools_pkey PRIMARY KEY (log_id, tool_id)
);

CREATE TABLE public.repair_logs (
    id serial NOT NULL,
    record_id integer NOT NULL,
    part_id integer,
    work_desc text,
    faults text,
    raw_data text,
    dt timestamptz DEFAULT now() NOT NULL,
    CONSTRAINT repair_logs_pkey PRIMARY KEY (id)
);

CREATE TABLE public.repair_records (
    id serial NOT NULL,
    user_id integer NOT NULL,
    item_id integer NOT NULL,
    first_mention timestamptz DEFAULT now() NOT NULL,
    last_update timestamptz DEFAULT now() NOT NULL,
    CONSTRAINT repair_records_pkey PRIMARY KEY (id)
);

CREATE TABLE public.resources (
    id serial NOT NULL,
    nazov text NOT NULL,
    esp text,
    pin text,
    led text,
    status text,
    vypozicane_komu text,
    created_at timestamptz DEFAULT now() NOT NULL,
    updated_at timestamptz DEFAULT now() NOT NULL,
    deleted boolean DEFAULT false NOT NULL,
    CONSTRAINT resources_pkey PRIMARY KEY (id)
);

CREATE TABLE public.users (
    id serial NOT NULL,
    first_name text NOT NULL,
    last_name text NOT NULL,
    email text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
