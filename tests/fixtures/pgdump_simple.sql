--
-- PostgreSQL database dump
--

-- Dumped from database version 16.0
-- Dumped by pg_dump version 16.0

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Data for Name: pg_loader_test_things; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.pg_loader_test_things (id, name, note) FROM stdin;
1	widget	\N
2	gizmo	hello
3	gadget	has\ttab
\.


--
-- PostgreSQL database dump complete
--

