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

CREATE TYPE public.resource_status AS ENUM (
    'AVAILABLE',
    'BORROWED',
    'LOST',
    'BROKEN'
);

CREATE TABLE public.resources (
    id serial NOT NULL,
    name text NOT NULL,
    esp text,
    pin text,
    led text,
    status public.resource_status,
    borrowed_by text,
    created_at timestamptz DEFAULT now() NOT NULL,
    updated_at timestamptz DEFAULT now() NOT NULL,
    deleted boolean DEFAULT false NOT NULL,
    CONSTRAINT resources_pkey PRIMARY KEY (id)
);

CREATE TABLE public.doc_units (
    id bigserial NOT NULL,
    doc_id text NOT NULL,
    manual_name text NOT NULL,
    source_path text NOT NULL,
    source_type text NOT NULL,
    unit_type text NOT NULL,
    unit_no integer,
    start_page integer,
    end_page integer,
    title text,
    heading_path text,
    summary text,
    text text NOT NULL,
    created_at timestamptz DEFAULT now(),
    search_vector tsvector GENERATED ALWAYS AS (
        to_tsvector(
            'simple',
            coalesce(title, '') || ' ' || coalesce(summary, '') || ' ' || coalesce(text, '')
        )
    ) STORED,
    CONSTRAINT doc_units_pkey PRIMARY KEY (id)
);

CREATE TABLE public.drive_documents (
    source_folder_id text NOT NULL,
    file_id text NOT NULL,
    doc_id text,
    local_path text NOT NULL,
    mime_type text NOT NULL,
    sync_token text,
    modified_time timestamptz,
    synced_at timestamptz DEFAULT now() NOT NULL,
    ingested_at timestamptz,
    ingest_status text DEFAULT 'pending' NOT NULL,
    ingest_error text,
    deleted boolean DEFAULT false NOT NULL,
    CONSTRAINT drive_documents_pkey PRIMARY KEY (source_folder_id, file_id),
    CONSTRAINT drive_documents_ingest_status_check CHECK (
        ingest_status IN ('pending', 'ok', 'failed', 'skipped')
    )
);

CREATE TABLE public.users (
    id serial NOT NULL,
    first_name text NOT NULL,
    last_name text NOT NULL,
    email text,
    CONSTRAINT users_pkey PRIMARY KEY (id)
);
