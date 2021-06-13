--
-- PostgreSQL database dump
--

-- Dumped from database version 10.11 (Debian 10.11-1.pgdg90+1)
-- Dumped by pg_dump version 12.1 (Debian 12.1-1.pgdg90+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: file_fdw; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS file_fdw WITH SCHEMA public;


--
-- Name: EXTENSION file_fdw; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION file_fdw IS 'foreign-data wrapper for flat file access';


--
-- Name: financial; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS financial WITH SCHEMA public;


--
-- Name: EXTENSION financial; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION financial IS 'Financial aggregate functions';


--
-- Name: tablefunc; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS tablefunc WITH SCHEMA public;


--
-- Name: EXTENSION tablefunc; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION tablefunc IS 'functions that manipulate whole tables, including crosstab';


--
-- Name: ffdw; Type: SERVER; Schema: -; Owner: postgres
--

CREATE SERVER ffdw FOREIGN DATA WRAPPER file_fdw;


ALTER SERVER ffdw OWNER TO postgres;

SET default_tablespace = '';

--
-- Name: date; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.date (
    seq integer,
    date date
);


ALTER TABLE public.date OWNER TO postgres;

--
-- Name: hist; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.hist (
    market text NOT NULL,
    code text NOT NULL,
    date date NOT NULL,
    open numeric(12,4),
    high numeric(12,4),
    low numeric(12,4),
    nav numeric(12,4),
    adjclose numeric(12,4),
    volume bigint
);


ALTER TABLE public.hist OWNER TO postgres;

--
-- Name: hist5d; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.hist5d AS
 SELECT h.market,
    h.code,
    h.date,
    ((d.seq - m.seq) + 1) AS period,
    h.nav,
    h.open,
    h.high,
    h.low,
    (((max(h.nav) OVER p_market_code_o_date_curr_row - h.nav) * (100)::numeric) / max(h.nav) OVER p_market_code_o_date_curr_row) AS dd,
    (((h.nav - max(h.nav) OVER p_market_code_o_date_all_dates) * '-100.0'::numeric) / max(h.nav) OVER p_market_code_o_date_all_dates) AS pct_max_all,
    (((h.nav - min(h.nav) OVER p_market_code_o_date_all_dates) * 100.0) / min(h.nav) OVER p_market_code_o_date_all_dates) AS pct_min_all,
    (((h.nav - max(h.nav) OVER p_market_code_o_date_52w) * '-100.0'::numeric) / max(h.nav) OVER p_market_code_o_date_52w) AS pct_max_52w,
    (((h.nav - min(h.nav) OVER p_market_code_o_date_52w) * 100.0) / min(h.nav) OVER p_market_code_o_date_52w) AS pct_min_52w,
    h.volume,
    m.date AS maxdate
   FROM public.hist h,
    public.date d,
    ( SELECT i.market,
            i.code,
            i.date,
            d_1.seq
           FROM ( SELECT i_1.market,
                    i_1.code,
                    max(i_1.date) AS date
                   FROM public.hist i_1,
                    ( SELECT max(date.date) AS max_cal_date
                           FROM public.date) d_2
                  WHERE (i_1.date >= (d_2.max_cal_date - 9))
                  GROUP BY i_1.market, i_1.code) i,
            public.date d_1
          WHERE (i.date = d_1.date)) m
  WHERE ((0 = 0) AND (h.date = d.date) AND (h.market = m.market) AND (h.code = m.code) AND (d.seq IS NOT NULL) AND ((((d.seq - m.seq) % 20) = 0) OR (((d.seq - m.seq) + 1) = ANY (ARRAY[1, 2, 6, 11]))))
  WINDOW p_market_code_o_date_curr_row AS (PARTITION BY h.market, h.code ORDER BY h.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), p_market_code_o_date_all_dates AS (PARTITION BY h.market, h.code ORDER BY h.date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), p_market_code_o_date_52w AS (PARTITION BY h.market, h.code ORDER BY h.date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW)
  WITH NO DATA;


ALTER TABLE public.hist5d OWNER TO postgres;

--
-- Name: asset; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.asset AS
 SELECT i.market,
    i.code,
    i.period,
    i.date AS dt,
    i.maxdate AS date,
    i.nav,
    i.open,
    i.high,
    i.low,
    (((i.nav - lead(i.nav, 1) OVER market_code_period) * (100)::numeric) / lead(i.nav, 1) OVER market_code_period) AS ret,
    i.dd,
    i.pct_max_all,
    i.pct_min_all,
    i.pct_max_52w,
    i.pct_min_52w,
    i.volume,
    0.7 AS rfr
   FROM public.hist5d i
  WHERE (i.period <> ALL (ARRAY[2, 6]))
  WINDOW market_code_period AS (PARTITION BY i.market, i.code ORDER BY i.market, i.code, i.period)
  WITH NO DATA;


ALTER TABLE public.asset OWNER TO postgres;

--
-- Name: date5m; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.date5m (
    seq bigint,
    date timestamp without time zone
);


ALTER TABLE public.date5m OWNER TO postgres;

--
-- Name: hist5m; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.hist5m (
    market text NOT NULL,
    code text NOT NULL,
    date timestamp without time zone NOT NULL,
    open numeric(12,4),
    high numeric(12,4),
    low numeric(12,4),
    nav numeric(12,4),
    volume bigint,
    adjclose numeric(12,4)
);


ALTER TABLE public.hist5m OWNER TO postgres;

--
-- Name: master; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.master (
    market text NOT NULL,
    code text NOT NULL,
    name text,
    isin text,
    type text,
    category text,
    sector text,
    subsector text
);


ALTER TABLE public.master OWNER TO postgres;

--
-- Name: master_fndmtl; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.master_fndmtl (
    isin character varying,
    mcap numeric(12,4),
    pe_curr numeric(12,4),
    pe_avg numeric(12,4),
    pb_curr numeric(12,4),
    pb_avg numeric(12,4),
    peg numeric(12,4),
    div_yield numeric(12,4)
);


ALTER TABLE public.master_fndmtl OWNER TO postgres;

--
-- Name: portfolio; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.portfolio (
    channel text,
    market text,
    code text,
    isin text,
    action text,
    qty numeric(12,4),
    price numeric(12,4),
    date date
);
ALTER TABLE ONLY public.portfolio ALTER COLUMN channel SET STORAGE MAIN;
ALTER TABLE ONLY public.portfolio ALTER COLUMN market SET STORAGE MAIN;
ALTER TABLE ONLY public.portfolio ALTER COLUMN code SET STORAGE MAIN;
ALTER TABLE ONLY public.portfolio ALTER COLUMN isin SET STORAGE MAIN;
ALTER TABLE ONLY public.portfolio ALTER COLUMN action SET STORAGE MAIN;


ALTER TABLE public.portfolio OWNER TO postgres;

--
-- Name: hist5m hist5m_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hist5m
    ADD CONSTRAINT hist5m_pkey PRIMARY KEY (code, date, market);


--
-- Name: hist hist_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hist
    ADD CONSTRAINT hist_pkey PRIMARY KEY (code, date, market);


--
-- Name: master master_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.master
    ADD CONSTRAINT master_pkey PRIMARY KEY (code, market);


--
-- Name: asset_market_code; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX asset_market_code ON public.asset USING btree (market, code);


--
-- Name: date5m_seq_date; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX date5m_seq_date ON public.date5m USING btree (seq, date);


--
-- Name: date_date_seq; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX date_date_seq ON public.date USING btree (date, seq);


--
-- Name: date_seq_date; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX date_seq_date ON public.date USING btree (seq, date);


--
-- Name: hist5d_market_code_period; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX hist5d_market_code_period ON public.hist5d USING btree (market, code, period);


--
-- Name: hist5d_period_market; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX hist5d_period_market ON public.hist5d USING btree (period, market);


--
-- Name: hist5m_market; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX hist5m_market ON public.hist5m USING brin (market);


--
-- Name: hist_date; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX hist_date ON public.hist USING btree (date);


--
-- Name: hist_market; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX hist_market ON public.hist USING brin (market);


--
-- Name: master_fmtl_pkey; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX master_fmtl_pkey ON public.master_fndmtl USING btree (isin);


--
-- Name: master_market; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX master_market ON public.master USING brin (market);


--
-- PostgreSQL database dump complete
--

