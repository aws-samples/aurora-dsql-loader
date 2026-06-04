--
-- PostgreSQL database dump
--
-- Synthetic full-dump fixture for the loader's `migrate` flow. Mirrors the
-- shape `pg_dump -Fp` (no --data-only) emits for a real schema that uses:
--   * SERIAL primary key   (the 4-statement expansion that dsql-lint collapses)
--   * NOT NULL DEFAULT ''  (a rule dsql-lint preserves)
--   * UNIQUE constraint    (allowed in DSQL, no transform needed)
--   * FOREIGN KEY          (auto-removed by dsql-lint with a warning)
--   * sync CREATE INDEX    (dsql-lint rewrites to CREATE INDEX ASYNC)
-- Used by the orchestrator's offline smoke test to pin --dry-run behavior
-- without needing a real Postgres source. See
-- src/migrate/orchestrator.rs::dry_run_full_dump_fixture for assertions.
--

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;

SET default_tablespace = '';
SET default_table_access_method = heap;

--
-- Name: events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.events (
    id integer NOT NULL,
    label text NOT NULL,
    note text DEFAULT ''::text NOT NULL,
    user_id integer
);

--
-- Name: events_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.events_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.events_id_seq OWNED BY public.events.id;

--
-- Name: users; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.users (
    id integer NOT NULL,
    email text NOT NULL UNIQUE
);

CREATE SEQUENCE public.users_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.users_id_seq OWNED BY public.users.id;

--
-- Name: events id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.events ALTER COLUMN id SET DEFAULT nextval('public.events_id_seq'::regclass);

--
-- Name: users id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users ALTER COLUMN id SET DEFAULT nextval('public.users_id_seq'::regclass);

--
-- Data for Name: events; Type: TABLE DATA; Schema: public
--

COPY public.events (id, label, note, user_id) FROM stdin;
1	alpha	first	1
2	beta		2
3	gamma	gamma-note	\N
\.

--
-- Data for Name: users; Type: TABLE DATA; Schema: public
--

COPY public.users (id, email) FROM stdin;
1	a@example.com
2	b@example.com
\.

--
-- Name: events events_pkey; Type: CONSTRAINT
--

ALTER TABLE ONLY public.events
    ADD CONSTRAINT events_pkey PRIMARY KEY (id);

--
-- Name: users users_pkey; Type: CONSTRAINT
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);

--
-- Name: events_label_idx; Type: INDEX
--

CREATE INDEX events_label_idx ON public.events USING btree (label);

--
-- Name: events events_user_id_fkey; Type: FK CONSTRAINT
--

ALTER TABLE ONLY public.events
    ADD CONSTRAINT events_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id);

--
-- PostgreSQL database dump complete
--
