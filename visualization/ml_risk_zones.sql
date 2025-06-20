--
-- PostgreSQL database dump
--

-- Dumped from database version 17.4 (Postgres.app)
-- Dumped by pg_dump version 17.0

-- Started on 2025-06-20 19:49:46 CEST

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 222 (class 1259 OID 3563783)
-- Name: ml_risk_zones; Type: TABLE; Schema: public; Owner: saad
--

CREATE TABLE public.ml_risk_zones (
    risk_zone integer,
    accident_count bigint,
    avg_severity double precision,
    center_lat double precision,
    center_lng double precision,
    risk_level text
);


ALTER TABLE public.ml_risk_zones OWNER TO saad;

--
-- TOC entry 3679 (class 0 OID 3563783)
-- Dependencies: 222
-- Data for Name: ml_risk_zones; Type: TABLE DATA; Schema: public; Owner: saad
--

COPY public.ml_risk_zones (risk_zone, accident_count, avg_severity, center_lat, center_lng, risk_level) FROM stdin;
0	20023	2.284372971083254	41.1222227475273	-88.46603180237277	MEDIUM
1	32468	2.235277811999507	39.769208453949254	-76.0443440576676	MEDIUM
2	35944	2.1669263298464276	30.977031962158375	-81.9613118229481	MEDIUM
3	41745	2.1851000119774824	37.01658129094583	-118.71516402012261	MEDIUM
4	19820	2.195509586276488	32.26411034105859	-96.64416592795165	MEDIUM
\.


-- Completed on 2025-06-20 19:49:46 CEST

--
-- PostgreSQL database dump complete
--

