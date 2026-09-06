-- Sanitized PMS 9.5.2 schema fixture: definitions only, no source data or roles.
SET search_path = public, pg_catalog;
CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;

CREATE TABLE alarmplateinfo (
    id integer NOT NULL,
    authority character varying(64),
    reason character varying(64),
    begintime timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    endtime timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    plateno character varying(16),
    platetype smallint,
    platecolor smallint,
    vehicletype smallint,
    vehiclecolor smallint,
    alarmpriority smallint,
    alarmtype smallint,
    alarmplan text,
    alarmperson character varying(32),
    contact character varying(32),
    contactnumber character varying(32),
    enableflag smallint DEFAULT 0,
    alarmproperty smallint,
    alarmdescription text,
    centerid integer,
    "timestamp" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    reserve1 integer,
    reserve2 character varying(64)
);

CREATE TABLE cardceilpayinfo (
    id integer NOT NULL,
    cardtype smallint,
    modtime timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    chargetype smallint,
    cardmoney integer,
    userid integer,
    username text,
    description text,
    reserve1 integer,
    reserve2 character varying(64)
);

CREATE TABLE cardchargeinfo (
    id integer NOT NULL,
    uniqueno text,
    type integer,
    money integer,
    "time" integer,
    uniqueid text,
    userid integer,
    username text,
    plateno text,
    cardno text,
    "timestamp" timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone,
    reserve1 integer,
    reserve2 character varying(64),
    description text
);

CREATE TABLE cardcostinfo (
    id integer NOT NULL,
    type smallint,
    cost integer,
    centerid integer,
    "timestamp" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone
);

CREATE TABLE cardfreetimeinfo (
    id integer NOT NULL,
    uniquenumber character varying(32),
    freetimeused integer,
    centerid integer,
    "timestamp" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone
);

CREATE TABLE cardinfo (
    id integer NOT NULL,
    cardno character varying(32),
    cardtype smallint,
    begintime timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    endtime timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    cardstate smallint,
    registerlosstime timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    remainingsum integer,
    cardreadertype smallint,
    department character varying(32),
    platenobackup character varying(16),
    payruleid integer,
    reduceruleid integer,
    chargetype smallint,
    cardcost integer,
    freetimeused integer,
    uniquenumber character varying(32),
    centerid integer,
    "timestamp" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    reserve1 integer,
    reserve2 character varying(64),
    parkpermission text,
    senderpeople text,
    userphone text,
    encryptcardno text
);

CREATE TABLE cardupdateinfo (
    id integer NOT NULL,
    operatorid integer,
    operatorname character varying(32),
    type smallint,
    cardno character varying(32),
    cardcost integer,
    "timestamp" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    uploadflag smallint
);

CREATE TABLE categorypayrulemap (
    id integer NOT NULL,
    categoryid integer,
    payruleid integer,
    centerid integer,
    "timestamp" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    parkbelonged integer,
    temppayruleid integer DEFAULT 0
);

CREATE TABLE chargeruleinfo (
    id integer NOT NULL,
    name text,
    money integer,
    chargemonth integer,
    deleteflag integer,
    description text,
    "timestamp" timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone,
    reserve1 integer,
    reserve2 character varying(64)
);

CREATE TABLE charginginfo (
    id integer NOT NULL,
    plateno character varying(16),
    cardno character varying(32),
    gateno integer,
    outtime timestamp(6) without time zone,
    intime timestamp(6) without time zone,
    chargedamount integer,
    operatorid integer,
    operatorname character varying(64),
    payruleid integer,
    reduceruleid integer,
    vehicletype smallint,
    description text,
    inuniqueid character varying(32),
    outuniqueid character varying(32),
    chargetype smallint,
    checkserial character varying(32),
    reducemoney integer,
    parkingduration integer,
    uploadflag smallint,
    "timestamp" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    reserve1 integer,
    reserve2 character varying(64),
    passtype integer,
    forceruleid integer,
    forcerulename character varying(32),
    clienthash text,
    parkbelonged integer,
    picinpath text,
    picoutpath text,
    parkingtype integer DEFAULT 1,
    reduceentryserial text DEFAULT ''::text,
    couponcode character varying(32),
    deductmoney integer,
    billcode character varying(32),
    thirdbillno character varying(32),
    thirdpaytype integer,
    terminal_no character varying(32),
    terminal_tran_no character varying(32),
    tran_no character varying(32),
    tac character varying(32),
    etcserial character varying(32),
    out_trade_no character varying(64),
    periodlimit integer DEFAULT 0,
    periodfreetime integer DEFAULT 0,
    uploadcmpflag integer DEFAULT 0
);

CREATE TABLE clientlist (
    id integer NOT NULL,
    clienthash text,
    clientip text,
    clientloginuser text,
    clienthttpprefix text,
    autologinuserid integer,
    savedpassword character varying(64),
    autologinflag smallint,
    updatetime timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    reserve1 integer,
    reserve2 character varying(64)
);

CREATE TABLE controllercardinfo (
    id integer NOT NULL,
    cardno character varying(64),
    controllerid integer,
    reserve1 integer,
    reserve2 character varying(64),
    clienthash text,
    parkbelonged integer
);

CREATE TABLE coupontable (
    id integer NOT NULL,
    deductrulename character varying(32),
    couponcode character varying(32),
    deductruleuuid character varying(32),
    deductruletype smallint,
    deductcontent smallint,
    description text,
    buytime timestamp without time zone,
    starttime timestamp without time zone,
    endtime timestamp without time zone,
    deleteflag smallint,
    "timestamp" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    remark text,
    active smallint,
    reserve1 integer,
    reserve2 character varying(64)
);

CREATE TABLE currentturn (
    id integer NOT NULL,
    operatorid integer,
    operatorname text,
    begintime timestamp without time zone,
    endtime timestamp without time zone,
    amounttotal integer,
    amountcash integer,
    amountcard integer,
    amountonecard integer,
    amountcut integer,
    countvehicle integer,
    mapcut text,
    "timestamp" timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone,
    clienthash text,
    amountthird integer DEFAULT 0,
    amountetc integer DEFAULT 0
);

CREATE TABLE customdefine (
    id integer NOT NULL,
    type character varying(32),
    name character varying(64),
    code integer,
    reserve1 integer,
    reserve2 character varying(64)
);

CREATE TABLE deductmoneytable (
    id integer NOT NULL,
    unid character varying(32),
    settletime timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    deductmoney integer DEFAULT 0
);

CREATE TABLE deviceinfo (
    id integer NOT NULL,
    name character varying(32),
    serialportno character varying(12),
    baudrate integer,
    portindex integer,
    ipaddress character varying(16),
    port integer,
    channelno integer,
    lprflag integer DEFAULT 0,
    devicetype integer,
    devicedetailtype integer,
    devicemodel character varying(64),
    deviceserial text,
    deviceaddresstype smallint,
    tradename character varying(32),
    deviceuser character varying(32),
    devicepwd text,
    extra text,
    "timestamp" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    reserve1 integer,
    reserve2 character varying(64),
    clienthash text,
    leddisplay text
);

CREATE TABLE devicemap (
    id integer NOT NULL,
    type integer,
    relatedno integer,
    relatedid integer,
    deviceid integer,
    physicallaneno integer,
    centerid integer,
    "timestamp" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    reserve1 integer,
    reserve2 character varying(64),
    clienthash text
);

CREATE TABLE etclisttable (
    id integer NOT NULL,
    license character varying(12),
    cardid character varying(32),
    vehicletype character varying(12),
    category character varying(12),
    licensecolor character varying(12),
    "time" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone
);

CREATE TABLE etcresulttable (
    id integer NOT NULL,
    plateno character varying(32),
    billno character varying(32),
    inuniqueid character varying(32),
    totalfee integer DEFAULT 0,
    payflag integer DEFAULT 2,
    "time" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone
);

CREATE TABLE forcereleaseruleinfo (
    id integer NOT NULL,
    name character varying(32),
    amount integer,
    description text,
    begintime timestamp without time zone,
    endtime timestamp without time zone,
    deleteflag smallint,
    centerid integer,
    "timestamp" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    reserve1 integer,
    reserve2 character varying(64),
    isdefault integer DEFAULT 0,
    selectable integer DEFAULT 1,
    vehicletype integer DEFAULT 3
);

CREATE TABLE gateinfo (
    id integer NOT NULL,
    centerid integer,
    gateno integer,
    index character varying(16),
    name character varying(64),
    longitude character varying(10),
    latitude character varying(10),
    "timestamp" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    activeflag integer DEFAULT 0,
    reserve1 integer,
    reserve2 character varying(64),
    clienthash text,
    parkbelonged integer
);

CREATE TABLE holidayinfo (
    id integer NOT NULL,
    name text,
    date timestamp without time zone,
    centerid integer,
    "timestamp" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    reserve1 integer,
    reserve2 character varying(64)
);

CREATE TABLE holidays (
    id integer NOT NULL,
    specialdate date,
    workorholidays smallint DEFAULT 0
);

CREATE TABLE laneinfo (
    id integer NOT NULL,
    gateno integer,
    laneno integer,
    direction integer DEFAULT 0,
    name character varying(32),
    "timestamp" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    activeflag integer DEFAULT 0,
    chargingflag integer DEFAULT 0,
    alarmflag integer DEFAULT 0,
    alarmtype character varying(32),
    lprreleaseflag integer DEFAULT 0,
    manualreleaseflag integer DEFAULT 0,
    cardreleaseflag integer DEFAULT 0,
    physicalno integer,
    passmode smallint DEFAULT 0,
    innerpass smallint DEFAULT 1,
    temppass smallint DEFAULT 0,
    nonepass smallint DEFAULT 0,
    needmatched smallint DEFAULT 0,
    onetimepass smallint DEFAULT 0,
    innercardpass smallint DEFAULT 1,
    tempcardpass smallint DEFAULT 1,
    innerautopass smallint DEFAULT 1,
    tempautopass smallint DEFAULT 0,
    prepaidautocharge smallint DEFAULT 0,
    prepaidconfirmedcharge smallint DEFAULT 1,
    centerid integer,
    reserve1 integer,
    reserve2 character varying(64),
    clienthash text,
    chargeneedcard integer,
    specialpass integer,
    specialfreepass integer,
    flags text
);

CREATE TABLE lastamoutpayable (
    id integer NOT NULL,
    inunid character varying(32),
    settletime timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    amoutpayable integer DEFAULT 0
);

CREATE TABLE lastgroupcarinfo (
    id integer NOT NULL,
    groupbelonged integer DEFAULT 0,
    plateno character varying(32),
    detail bytea,
    "time" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone
);

CREATE TABLE leddisplayinfo (
    id integer NOT NULL,
    content text,
    ledid integer,
    type smallint,
    centerid integer,
    "timestamp" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    clienthash text,
    parkbelonged integer
);

CREATE TABLE localchargetable (
    id integer NOT NULL,
    inunid character varying(32),
    plateno character varying(32),
    outtime timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    money integer DEFAULT '-1'::integer,
    vehicletype integer DEFAULT 3,
    platetype integer DEFAULT 0,
    cardno character varying(32),
    status smallint DEFAULT 0
);

CREATE TABLE metainfo (
    id integer NOT NULL,
    metakey text,
    metavalue text,
    "timestamp" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    reserve1 integer,
    reserve2 character varying(64)
);

CREATE TABLE netconsynctable (
    id integer NOT NULL,
    tablename text,
    operationtype integer,
    operationserial integer,
    prevparkids text,
    curparkids text,
    operationtime timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    reserve1 integer,
    reserve2 character varying(64)
);

CREATE TABLE operationrecordinfo (
    id integer NOT NULL,
    operatorid integer,
    type smallint,
    operatorname character varying(32),
    cardno character varying(32),
    plateno character varying(16),
    description text,
    uniqueid character varying(32),
    "timestamp" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    uploadflag smallint,
    clienthash text
);

CREATE TABLE parkbindinginfotable (
    id integer NOT NULL,
    begintime timestamp without time zone,
    endtime timestamp without time zone,
    plateno character varying(32),
    platecolor integer,
    status integer,
    orderno character varying(32),
    type integer,
    deductmoney integer,
    "timestamp" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone
);

CREATE TABLE parkedvehicleinfo (
    id integer NOT NULL,
    vehicletype integer,
    platecolor integer,
    vehiclecolor integer,
    plateno character varying(16),
    cardno character varying(32),
    passtime timestamp without time zone,
    cardtime timestamp without time zone,
    pic text,
    plateurl text,
    gate character varying(32),
    lane character varying(32),
    uniqueid character varying(32),
    refno integer,
    centerid integer,
    "timestamp" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    clienthash text,
    parkbelonged integer,
    gateno integer,
    parkingtype integer,
    laneno integer,
    belief integer,
    mainlogo integer,
    sublogo integer,
    groupid integer DEFAULT 0,
    groupstatus integer DEFAULT 0
);

CREATE TABLE parkedvehiclepictable (
    id integer NOT NULL,
    uniqueid character varying(32),
    localpicpath text,
    clienthash text
);

CREATE TABLE parkinfo (
    id integer NOT NULL,
    name text,
    enabled integer,
    currentcount integer,
    totalcount integer,
    "timestamp" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    reserve1 integer,
    reserve2 character varying(64),
    fulloperation integer DEFAULT 0,
    parkingtypelot text
);

CREATE TABLE passfaceinfo (
    id integer NOT NULL,
    gateno integer,
    laneno integer,
    direction smallint,
    passtime timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    faceid integer,
    facescore smallint,
    facerectleft real,
    facerecttop real,
    facerectwidth real,
    facerectheight real,
    facepicpath text,
    backgroundpicpath text,
    uploadflag smallint DEFAULT 0,
    "timestamp" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    reserve1 integer,
    reserve2 character varying(64),
    parkbelonged integer,
    clienthash text
);

CREATE TABLE passvehicleinfo (
    id integer NOT NULL,
    gateno integer,
    laneno integer,
    plateno character varying(16),
    cardno character varying(32),
    direction smallint,
    operationtype smallint,
    operatorname character varying(64),
    platetype smallint,
    platecolor smallint,
    passtime timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    vehiclecolor smallint,
    vehicleshade smallint,
    parkingtype smallint,
    lprstate smallint,
    vehicletype smallint,
    cardreadingtime timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    alarmflag smallint DEFAULT 0,
    alarmreason character varying(64),
    picpath text,
    platepicpath text,
    chargingflag smallint DEFAULT 0,
    uniqueid character varying(32),
    handlestatus smallint,
    chargetime timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    extradata text,
    uploadflag smallint DEFAULT 0,
    "timestamp" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    reserve1 integer,
    reserve2 character varying(64),
    clienthash text,
    parkbelonged integer,
    belief integer,
    mainlogo integer,
    sublogo integer,
    groupid integer DEFAULT 0,
    groupstatus integer DEFAULT 0,
    paireduniqueid text DEFAULT ''::text,
    sameownervehicles text,
    ownername text DEFAULT ''::text,
    ownercontact text DEFAULT ''::text,
    openuuid character varying(32),
    pilotfacepicpath text,
    copilotfacepicpath text,
    isfullwaiting integer DEFAULT 0,
    parkingstatus integer DEFAULT 1,
    alternative integer DEFAULT 0,
    uploadcmpflag integer DEFAULT 0
);

CREATE TABLE payruleinfo (
    id integer NOT NULL,
    name character varying(32),
    vehicletype smallint,
    cardtype smallint,
    ruletype smallint,
    detail bytea,
    description text,
    begintime timestamp without time zone,
    endtime timestamp without time zone,
    deleteflag smallint,
    centerid integer,
    "timestamp" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    reserve1 integer,
    reserve2 character varying(64),
    templatename character varying(256),
    fullruledetail text,
    calcdetail text,
    holidaycharges integer DEFAULT 1
);

CREATE TABLE reduceentry (
    id integer NOT NULL,
    reduceruleid integer,
    status integer,
    "timestamp" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    validtime timestamp(6) without time zone,
    uuid text,
    operatorid integer
);

CREATE TABLE reduceruleinfo (
    id integer NOT NULL,
    name character varying(32),
    ruletype smallint,
    detail bytea,
    description text,
    begintime timestamp without time zone,
    endtime timestamp without time zone,
    deleteflag smallint,
    centerid integer,
    "timestamp" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    reserve1 integer,
    reserve2 character varying(64)
);

CREATE TABLE roleinfo (
    id integer NOT NULL,
    level smallint,
    description text,
    name character varying(64),
    createtime timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    begintime timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    expiretime timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    centerid integer,
    "timestamp" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    reserve1 integer,
    reserve2 character varying(64)
);

CREATE TABLE rolemodulemap (
    id integer NOT NULL,
    roleid integer,
    moduleid integer
);

CREATE TABLE shiftturnoverlog (
    id integer NOT NULL,
    operatorid integer,
    operatorname character varying(64),
    cardno character varying(32),
    begintime timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    endtime timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    totalamount integer,
    cutamount integer,
    totalvehiclenumber integer,
    uploadflag smallint DEFAULT 0,
    centerid integer,
    "timestamp" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    reserve1 integer,
    reserve2 character varying(64),
    clienthash text,
    gateid integer,
    cash integer,
    gatename character varying(32)
);

CREATE TABLE spotsgroupinfo (
    id integer NOT NULL,
    groupname text,
    spotsnum smallint,
    operationtime timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone
);

CREATE TABLE syncrecordinfo (
    id integer NOT NULL,
    tablename character varying(64),
    operationtype integer,
    recordserial integer,
    operationtime timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    reserve1 integer,
    reserve2 character varying(64)
);

CREATE TABLE syncskipinfo (
    id integer NOT NULL,
    tablename text,
    tabletype integer,
    tableindex bigint,
    errortimes integer,
    uploadflag smallint,
    "timestamp" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    reserve1 integer,
    reserve2 character varying(64)
);

CREATE TABLE thirdchargeinfo (
    id integer NOT NULL,
    type smallint,
    billno character varying(32),
    inserialno character varying(32),
    plateno character varying(16),
    summary text,
    amount integer,
    haveused smallint DEFAULT 0,
    chargetime timestamp without time zone,
    createtime timestamp(0) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    paystatus integer DEFAULT 1,
    thirdchargetype integer DEFAULT 0,
    reduce integer DEFAULT 0
);

CREATE TABLE thirdparamtable (
    id integer NOT NULL,
    key character varying(32),
    secret character varying(64),
    "time" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone
);

CREATE TABLE typedefine (
    id integer NOT NULL,
    type character varying(32),
    name character varying(64),
    value character varying(64),
    code integer,
    reserve1 integer,
    reserve2 character varying(64)
);

CREATE TABLE userinfo (
    id integer NOT NULL,
    name character varying(64),
    password character varying(64),
    roleid integer,
    bindingip character varying(16),
    bindingmac character varying(18),
    forbidflag smallint DEFAULT 0,
    autologinflag smallint DEFAULT 0,
    savedpassword character varying(64),
    activeflag smallint DEFAULT 0,
    centerid integer,
    "timestamp" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    reserve1 integer,
    reserve2 character varying(64)
);

CREATE TABLE vehiclebilltable (
    id integer NOT NULL,
    parkingindex character varying(32),
    unid character varying(32),
    billcode character varying(32),
    plateno character varying(32),
    platecolor integer,
    begintime timestamp without time zone,
    settletime timestamp without time zone,
    paytime timestamp without time zone,
    parkperiodtime integer,
    "timestamp" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    status integer,
    money integer,
    totalfee integer,
    alreadypay integer,
    description text,
    ispass integer,
    thirdbillno character varying(32),
    thirdpaytype integer,
    reserve1 integer,
    reserve2 character varying(64),
    directreduce integer DEFAULT 0,
    payruleid integer DEFAULT 0
);

CREATE TABLE vehiclecategoryinfo (
    id integer NOT NULL,
    name character varying(32),
    type smallint,
    centerid integer,
    expiredhandling smallint,
    matchedhandling smallint,
    maxfreetime integer,
    "timestamp" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    userid integer,
    username text,
    description text,
    daymaxpay integer DEFAULT 0,
    enabledaymaxpay integer DEFAULT 0,
    daylimitmode integer DEFAULT 0,
    daylimithour integer DEFAULT 0,
    daylimitdetail integer DEFAULT 0,
    lanerule text,
    iftakepark integer DEFAULT 1
);

CREATE TABLE vehicleinfo (
    id integer NOT NULL,
    plateno character varying(16),
    platecolor smallint,
    platetype smallint,
    vehiclecolor smallint,
    vehicletype smallint,
    parkingtype smallint,
    cardno character varying(32),
    brand character varying(32),
    ownername character varying(32),
    owneraddress character varying(64),
    ownerphonenumber character varying(32),
    identitynumber character varying(32),
    registertime timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    begintime timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    endtime timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    ownergender smallint,
    ownerworkplace character varying(64),
    ownerdepartment character varying(64),
    ownerpost character varying(32),
    uniquenumber character varying(32),
    categorybelonged integer,
    expiredhandling smallint,
    matchedhandling smallint,
    "timestamp" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    reserve1 integer,
    reserve2 character varying(64),
    parkpermission text,
    vehicleinfo_cardtype integer,
    groupbelonged integer DEFAULT 0,
    isalreaybag integer DEFAULT 0,
    isfromthirdsystem integer DEFAULT 0,
    uploadflag integer DEFAULT 0,
    historytime text,
    extrainfo text
);

CREATE TABLE vehiclepassruleinfo (
    id integer NOT NULL,
    type smallint DEFAULT 0,
    laneid integer DEFAULT 0,
    categoryid integer DEFAULT 0,
    detail bytea,
    reserve1 integer,
    reserve2 text
);

CREATE TABLE vehicletypeinfo (
    id integer NOT NULL,
    vehicletypename text,
    "timestamp" timestamp(6) without time zone DEFAULT ('now'::text)::timestamp without time zone,
    mark smallint DEFAULT 0
);

CREATE TABLE worklog (
    id integer NOT NULL,
    userid integer,
    username character varying(64),
    detail text,
    "timestamp" timestamp(6) without time zone,
    reserve1 integer,
    reserve2 character varying(64)
);

CREATE SEQUENCE alarmplateinfo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE cardceilpayinfo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE cardchargeinfo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE cardcostinfo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE cardfreetimeinfo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE cardinfo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE cardupdateinfo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE categorypayrulemap_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE chargeruleinfo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE charginginfo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE clientlist_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE controllercardinfo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE coupontable_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE currentturn_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE customdefine_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE deductmoneytable_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE deviceinfo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE devicemap_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE etclisttable_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE etcresulttable_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE forcereleaseruleinfo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE gateinfo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE holidayinfo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE holidays_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE laneinfo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE lastamoutpayable_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE lastgroupcarinfo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE leddisplayinfo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE localchargetable_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE metainfo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE netconsynctable_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE operationrecordinfo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE parkbindinginfotable_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE parkedvehicleinfo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE parkedvehiclepictable_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE parkinfo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE passfaceinfo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE passvehicleinfo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE payruleinfo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE pms_special_string_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE reduceentry_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE reduceruleinfo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE roleinfo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE rolemodulemap_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE shiftturnoverlog_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE spotsgroupinfo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE syncrecordinfo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE syncskipinfo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE thirdchargeinfo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE thirdparamtable_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE typedefine_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE uniquenosequence
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE userinfo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE vehiclebilltable_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE vehiclecategoryinfo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE vehicleinfo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE vehiclepassruleinfo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE vehicletypeinfo_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE SEQUENCE worklog_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE alarmplateinfo_id_seq OWNED BY alarmplateinfo.id;
ALTER SEQUENCE cardceilpayinfo_id_seq OWNED BY cardceilpayinfo.id;
ALTER SEQUENCE cardchargeinfo_id_seq OWNED BY cardchargeinfo.id;
ALTER SEQUENCE cardcostinfo_id_seq OWNED BY cardcostinfo.id;
ALTER SEQUENCE cardfreetimeinfo_id_seq OWNED BY cardfreetimeinfo.id;
ALTER SEQUENCE cardinfo_id_seq OWNED BY cardinfo.id;
ALTER SEQUENCE cardupdateinfo_id_seq OWNED BY cardupdateinfo.id;
ALTER SEQUENCE categorypayrulemap_id_seq OWNED BY categorypayrulemap.id;
ALTER SEQUENCE chargeruleinfo_id_seq OWNED BY chargeruleinfo.id;
ALTER SEQUENCE charginginfo_id_seq OWNED BY charginginfo.id;
ALTER SEQUENCE clientlist_id_seq OWNED BY clientlist.id;
ALTER SEQUENCE controllercardinfo_id_seq OWNED BY controllercardinfo.id;
ALTER SEQUENCE coupontable_id_seq OWNED BY coupontable.id;
ALTER SEQUENCE currentturn_id_seq OWNED BY currentturn.id;
ALTER SEQUENCE customdefine_id_seq OWNED BY customdefine.id;
ALTER SEQUENCE deductmoneytable_id_seq OWNED BY deductmoneytable.id;
ALTER SEQUENCE deviceinfo_id_seq OWNED BY deviceinfo.id;
ALTER SEQUENCE devicemap_id_seq OWNED BY devicemap.id;
ALTER SEQUENCE etclisttable_id_seq OWNED BY etclisttable.id;
ALTER SEQUENCE etcresulttable_id_seq OWNED BY etcresulttable.id;
ALTER SEQUENCE forcereleaseruleinfo_id_seq OWNED BY forcereleaseruleinfo.id;
ALTER SEQUENCE gateinfo_id_seq OWNED BY gateinfo.id;
ALTER SEQUENCE holidayinfo_id_seq OWNED BY holidayinfo.id;
ALTER SEQUENCE holidays_id_seq OWNED BY holidays.id;
ALTER SEQUENCE laneinfo_id_seq OWNED BY laneinfo.id;
ALTER SEQUENCE lastamoutpayable_id_seq OWNED BY lastamoutpayable.id;
ALTER SEQUENCE lastgroupcarinfo_id_seq OWNED BY lastgroupcarinfo.id;
ALTER SEQUENCE leddisplayinfo_id_seq OWNED BY leddisplayinfo.id;
ALTER SEQUENCE localchargetable_id_seq OWNED BY localchargetable.id;
ALTER SEQUENCE metainfo_id_seq OWNED BY metainfo.id;
ALTER SEQUENCE netconsynctable_id_seq OWNED BY netconsynctable.id;
ALTER SEQUENCE operationrecordinfo_id_seq OWNED BY operationrecordinfo.id;
ALTER SEQUENCE parkbindinginfotable_id_seq OWNED BY parkbindinginfotable.id;
ALTER SEQUENCE parkedvehicleinfo_id_seq OWNED BY parkedvehicleinfo.id;
ALTER SEQUENCE parkedvehiclepictable_id_seq OWNED BY parkedvehiclepictable.id;
ALTER SEQUENCE parkinfo_id_seq OWNED BY parkinfo.id;
ALTER SEQUENCE passfaceinfo_id_seq OWNED BY passfaceinfo.id;
ALTER SEQUENCE passvehicleinfo_id_seq OWNED BY passvehicleinfo.id;
ALTER SEQUENCE payruleinfo_id_seq OWNED BY payruleinfo.id;
ALTER SEQUENCE reduceentry_id_seq OWNED BY reduceentry.id;
ALTER SEQUENCE reduceruleinfo_id_seq OWNED BY reduceruleinfo.id;
ALTER SEQUENCE roleinfo_id_seq OWNED BY roleinfo.id;
ALTER SEQUENCE rolemodulemap_id_seq OWNED BY rolemodulemap.id;
ALTER SEQUENCE shiftturnoverlog_id_seq OWNED BY shiftturnoverlog.id;
ALTER SEQUENCE spotsgroupinfo_id_seq OWNED BY spotsgroupinfo.id;
ALTER SEQUENCE syncrecordinfo_id_seq OWNED BY syncrecordinfo.id;
ALTER SEQUENCE syncskipinfo_id_seq OWNED BY syncskipinfo.id;
ALTER SEQUENCE thirdchargeinfo_id_seq OWNED BY thirdchargeinfo.id;
ALTER SEQUENCE thirdparamtable_id_seq OWNED BY thirdparamtable.id;
ALTER SEQUENCE typedefine_id_seq OWNED BY typedefine.id;
ALTER SEQUENCE userinfo_id_seq OWNED BY userinfo.id;
ALTER SEQUENCE vehiclebilltable_id_seq OWNED BY vehiclebilltable.id;
ALTER SEQUENCE vehiclecategoryinfo_id_seq OWNED BY vehiclecategoryinfo.id;
ALTER SEQUENCE vehicleinfo_id_seq OWNED BY vehicleinfo.id;
ALTER SEQUENCE vehiclepassruleinfo_id_seq OWNED BY vehiclepassruleinfo.id;
ALTER SEQUENCE vehicletypeinfo_id_seq OWNED BY vehicletypeinfo.id;
ALTER SEQUENCE worklog_id_seq OWNED BY worklog.id;

ALTER TABLE ONLY alarmplateinfo ALTER COLUMN id SET DEFAULT nextval('alarmplateinfo_id_seq'::regclass);

ALTER TABLE ONLY cardceilpayinfo ALTER COLUMN id SET DEFAULT nextval('cardceilpayinfo_id_seq'::regclass);

ALTER TABLE ONLY cardchargeinfo ALTER COLUMN id SET DEFAULT nextval('cardchargeinfo_id_seq'::regclass);

ALTER TABLE ONLY cardcostinfo ALTER COLUMN id SET DEFAULT nextval('cardcostinfo_id_seq'::regclass);

ALTER TABLE ONLY cardfreetimeinfo ALTER COLUMN id SET DEFAULT nextval('cardfreetimeinfo_id_seq'::regclass);

ALTER TABLE ONLY cardinfo ALTER COLUMN id SET DEFAULT nextval('cardinfo_id_seq'::regclass);

ALTER TABLE ONLY cardupdateinfo ALTER COLUMN id SET DEFAULT nextval('cardupdateinfo_id_seq'::regclass);

ALTER TABLE ONLY categorypayrulemap ALTER COLUMN id SET DEFAULT nextval('categorypayrulemap_id_seq'::regclass);

ALTER TABLE ONLY chargeruleinfo ALTER COLUMN id SET DEFAULT nextval('chargeruleinfo_id_seq'::regclass);

ALTER TABLE ONLY charginginfo ALTER COLUMN id SET DEFAULT nextval('charginginfo_id_seq'::regclass);

ALTER TABLE ONLY clientlist ALTER COLUMN id SET DEFAULT nextval('clientlist_id_seq'::regclass);

ALTER TABLE ONLY controllercardinfo ALTER COLUMN id SET DEFAULT nextval('controllercardinfo_id_seq'::regclass);

ALTER TABLE ONLY coupontable ALTER COLUMN id SET DEFAULT nextval('coupontable_id_seq'::regclass);

ALTER TABLE ONLY currentturn ALTER COLUMN id SET DEFAULT nextval('currentturn_id_seq'::regclass);

ALTER TABLE ONLY customdefine ALTER COLUMN id SET DEFAULT nextval('customdefine_id_seq'::regclass);

ALTER TABLE ONLY deductmoneytable ALTER COLUMN id SET DEFAULT nextval('deductmoneytable_id_seq'::regclass);

ALTER TABLE ONLY deviceinfo ALTER COLUMN id SET DEFAULT nextval('deviceinfo_id_seq'::regclass);

ALTER TABLE ONLY devicemap ALTER COLUMN id SET DEFAULT nextval('devicemap_id_seq'::regclass);

ALTER TABLE ONLY etclisttable ALTER COLUMN id SET DEFAULT nextval('etclisttable_id_seq'::regclass);

ALTER TABLE ONLY etcresulttable ALTER COLUMN id SET DEFAULT nextval('etcresulttable_id_seq'::regclass);

ALTER TABLE ONLY forcereleaseruleinfo ALTER COLUMN id SET DEFAULT nextval('forcereleaseruleinfo_id_seq'::regclass);

ALTER TABLE ONLY gateinfo ALTER COLUMN id SET DEFAULT nextval('gateinfo_id_seq'::regclass);

ALTER TABLE ONLY holidayinfo ALTER COLUMN id SET DEFAULT nextval('holidayinfo_id_seq'::regclass);

ALTER TABLE ONLY holidays ALTER COLUMN id SET DEFAULT nextval('holidays_id_seq'::regclass);

ALTER TABLE ONLY laneinfo ALTER COLUMN id SET DEFAULT nextval('laneinfo_id_seq'::regclass);

ALTER TABLE ONLY lastamoutpayable ALTER COLUMN id SET DEFAULT nextval('lastamoutpayable_id_seq'::regclass);

ALTER TABLE ONLY lastgroupcarinfo ALTER COLUMN id SET DEFAULT nextval('lastgroupcarinfo_id_seq'::regclass);

ALTER TABLE ONLY leddisplayinfo ALTER COLUMN id SET DEFAULT nextval('leddisplayinfo_id_seq'::regclass);

ALTER TABLE ONLY localchargetable ALTER COLUMN id SET DEFAULT nextval('localchargetable_id_seq'::regclass);

ALTER TABLE ONLY metainfo ALTER COLUMN id SET DEFAULT nextval('metainfo_id_seq'::regclass);

ALTER TABLE ONLY netconsynctable ALTER COLUMN id SET DEFAULT nextval('netconsynctable_id_seq'::regclass);

ALTER TABLE ONLY operationrecordinfo ALTER COLUMN id SET DEFAULT nextval('operationrecordinfo_id_seq'::regclass);

ALTER TABLE ONLY parkbindinginfotable ALTER COLUMN id SET DEFAULT nextval('parkbindinginfotable_id_seq'::regclass);

ALTER TABLE ONLY parkedvehicleinfo ALTER COLUMN id SET DEFAULT nextval('parkedvehicleinfo_id_seq'::regclass);

ALTER TABLE ONLY parkedvehiclepictable ALTER COLUMN id SET DEFAULT nextval('parkedvehiclepictable_id_seq'::regclass);

ALTER TABLE ONLY parkinfo ALTER COLUMN id SET DEFAULT nextval('parkinfo_id_seq'::regclass);

ALTER TABLE ONLY passfaceinfo ALTER COLUMN id SET DEFAULT nextval('passfaceinfo_id_seq'::regclass);

ALTER TABLE ONLY passvehicleinfo ALTER COLUMN id SET DEFAULT nextval('passvehicleinfo_id_seq'::regclass);

ALTER TABLE ONLY payruleinfo ALTER COLUMN id SET DEFAULT nextval('payruleinfo_id_seq'::regclass);

ALTER TABLE ONLY reduceentry ALTER COLUMN id SET DEFAULT nextval('reduceentry_id_seq'::regclass);

ALTER TABLE ONLY reduceruleinfo ALTER COLUMN id SET DEFAULT nextval('reduceruleinfo_id_seq'::regclass);

ALTER TABLE ONLY roleinfo ALTER COLUMN id SET DEFAULT nextval('roleinfo_id_seq'::regclass);

ALTER TABLE ONLY rolemodulemap ALTER COLUMN id SET DEFAULT nextval('rolemodulemap_id_seq'::regclass);

ALTER TABLE ONLY shiftturnoverlog ALTER COLUMN id SET DEFAULT nextval('shiftturnoverlog_id_seq'::regclass);

ALTER TABLE ONLY spotsgroupinfo ALTER COLUMN id SET DEFAULT nextval('spotsgroupinfo_id_seq'::regclass);

ALTER TABLE ONLY syncrecordinfo ALTER COLUMN id SET DEFAULT nextval('syncrecordinfo_id_seq'::regclass);

ALTER TABLE ONLY syncskipinfo ALTER COLUMN id SET DEFAULT nextval('syncskipinfo_id_seq'::regclass);

ALTER TABLE ONLY thirdchargeinfo ALTER COLUMN id SET DEFAULT nextval('thirdchargeinfo_id_seq'::regclass);

ALTER TABLE ONLY thirdparamtable ALTER COLUMN id SET DEFAULT nextval('thirdparamtable_id_seq'::regclass);

ALTER TABLE ONLY typedefine ALTER COLUMN id SET DEFAULT nextval('typedefine_id_seq'::regclass);

ALTER TABLE ONLY userinfo ALTER COLUMN id SET DEFAULT nextval('userinfo_id_seq'::regclass);

ALTER TABLE ONLY vehiclebilltable ALTER COLUMN id SET DEFAULT nextval('vehiclebilltable_id_seq'::regclass);

ALTER TABLE ONLY vehiclecategoryinfo ALTER COLUMN id SET DEFAULT nextval('vehiclecategoryinfo_id_seq'::regclass);

ALTER TABLE ONLY vehicleinfo ALTER COLUMN id SET DEFAULT nextval('vehicleinfo_id_seq'::regclass);

ALTER TABLE ONLY vehiclepassruleinfo ALTER COLUMN id SET DEFAULT nextval('vehiclepassruleinfo_id_seq'::regclass);

ALTER TABLE ONLY vehicletypeinfo ALTER COLUMN id SET DEFAULT nextval('vehicletypeinfo_id_seq'::regclass);

ALTER TABLE ONLY worklog ALTER COLUMN id SET DEFAULT nextval('worklog_id_seq'::regclass);

ALTER TABLE ONLY alarmplateinfo
    ADD CONSTRAINT alarmplateinfo_pkey PRIMARY KEY (id);

ALTER TABLE ONLY cardchargeinfo
    ADD CONSTRAINT cardchargeinfo_pkey PRIMARY KEY (id);

ALTER TABLE ONLY cardcostinfo
    ADD CONSTRAINT cardcostinfo_pkey PRIMARY KEY (id);

ALTER TABLE ONLY cardfreetimeinfo
    ADD CONSTRAINT cardfreetimeinfo_pkey PRIMARY KEY (id);

ALTER TABLE ONLY cardinfo
    ADD CONSTRAINT cardinfo_pkey PRIMARY KEY (id);

ALTER TABLE ONLY cardupdateinfo
    ADD CONSTRAINT cardupdateinfo_pkey PRIMARY KEY (id);

ALTER TABLE ONLY categorypayrulemap
    ADD CONSTRAINT categorypayrulemap_pkey PRIMARY KEY (id);

ALTER TABLE ONLY chargeruleinfo
    ADD CONSTRAINT chargeruleinfo_pkey PRIMARY KEY (id);

ALTER TABLE ONLY charginginfo
    ADD CONSTRAINT charginginfo_pkey PRIMARY KEY (id);

ALTER TABLE ONLY clientlist
    ADD CONSTRAINT clientlist_pkey PRIMARY KEY (id);

ALTER TABLE ONLY controllercardinfo
    ADD CONSTRAINT controllercardinfo_pkey PRIMARY KEY (id);

ALTER TABLE ONLY coupontable
    ADD CONSTRAINT coupontable_pkey PRIMARY KEY (id);

ALTER TABLE ONLY currentturn
    ADD CONSTRAINT currentturn_pkey PRIMARY KEY (id);

ALTER TABLE ONLY customdefine
    ADD CONSTRAINT customdefine_pkey PRIMARY KEY (id);

ALTER TABLE ONLY deductmoneytable
    ADD CONSTRAINT deductmoneytable_pkey PRIMARY KEY (id);

ALTER TABLE ONLY deviceinfo
    ADD CONSTRAINT deviceinfo_pkey PRIMARY KEY (id);

ALTER TABLE ONLY devicemap
    ADD CONSTRAINT devicemap_pkey PRIMARY KEY (id);

ALTER TABLE ONLY etclisttable
    ADD CONSTRAINT etclisttable_pkey PRIMARY KEY (id);

ALTER TABLE ONLY etcresulttable
    ADD CONSTRAINT etcresulttable_billno_key UNIQUE (billno);

ALTER TABLE ONLY etcresulttable
    ADD CONSTRAINT etcresulttable_pkey PRIMARY KEY (id);

ALTER TABLE ONLY forcereleaseruleinfo
    ADD CONSTRAINT forcereleaseruleinfo_pkey PRIMARY KEY (id);

ALTER TABLE ONLY gateinfo
    ADD CONSTRAINT gateinfo_pkey PRIMARY KEY (id);

ALTER TABLE ONLY holidayinfo
    ADD CONSTRAINT holidayinfo_pkey PRIMARY KEY (id);

ALTER TABLE ONLY holidays
    ADD CONSTRAINT holidays_pkey PRIMARY KEY (id);

ALTER TABLE ONLY laneinfo
    ADD CONSTRAINT laneinfo_pkey PRIMARY KEY (id);

ALTER TABLE ONLY lastamoutpayable
    ADD CONSTRAINT lastamoutpayable_pkey PRIMARY KEY (id);

ALTER TABLE ONLY lastgroupcarinfo
    ADD CONSTRAINT lastgroupcarinfo_pkey PRIMARY KEY (id);

ALTER TABLE ONLY lastgroupcarinfo
    ADD CONSTRAINT lastgroupcarinfo_plateno_key UNIQUE (plateno);

ALTER TABLE ONLY leddisplayinfo
    ADD CONSTRAINT leddisplayinfo_pkey PRIMARY KEY (id);

ALTER TABLE ONLY localchargetable
    ADD CONSTRAINT localchargetable_pkey PRIMARY KEY (id);

ALTER TABLE ONLY netconsynctable
    ADD CONSTRAINT netconsynctable_pkey PRIMARY KEY (id);

ALTER TABLE ONLY operationrecordinfo
    ADD CONSTRAINT operationrecordinfo_pkey PRIMARY KEY (id);

ALTER TABLE ONLY parkbindinginfotable
    ADD CONSTRAINT parkbindinginfotable_pkey PRIMARY KEY (id);

ALTER TABLE ONLY parkedvehicleinfo
    ADD CONSTRAINT parkedvehicleinfo_pkey PRIMARY KEY (id);

ALTER TABLE ONLY parkedvehiclepictable
    ADD CONSTRAINT parkedvehiclepictable_pkey PRIMARY KEY (id);

ALTER TABLE ONLY parkinfo
    ADD CONSTRAINT parkinfo_pkey PRIMARY KEY (id);

ALTER TABLE ONLY passfaceinfo
    ADD CONSTRAINT passfaceinfo_pkey PRIMARY KEY (id);

ALTER TABLE ONLY passvehicleinfo
    ADD CONSTRAINT passvehicleinfo_pkey PRIMARY KEY (id);

ALTER TABLE ONLY payruleinfo
    ADD CONSTRAINT payruleinfo_pkey PRIMARY KEY (id);

ALTER TABLE ONLY reduceentry
    ADD CONSTRAINT reduceentry_pkey PRIMARY KEY (id);

ALTER TABLE ONLY reduceruleinfo
    ADD CONSTRAINT reduceruleinfo_pkey PRIMARY KEY (id);

ALTER TABLE ONLY roleinfo
    ADD CONSTRAINT roleinfo_pkey PRIMARY KEY (id);

ALTER TABLE ONLY rolemodulemap
    ADD CONSTRAINT rolemodulemap_pkey PRIMARY KEY (id);

ALTER TABLE ONLY shiftturnoverlog
    ADD CONSTRAINT shiftturnoverlog_pkey PRIMARY KEY (id);

ALTER TABLE ONLY spotsgroupinfo
    ADD CONSTRAINT spotsgroupinfo_pkey PRIMARY KEY (id);

ALTER TABLE ONLY syncrecordinfo
    ADD CONSTRAINT syncrecordinfo_pkey PRIMARY KEY (id);

ALTER TABLE ONLY thirdchargeinfo
    ADD CONSTRAINT thirdchargeinfo_pkey PRIMARY KEY (id);

ALTER TABLE ONLY thirdparamtable
    ADD CONSTRAINT thirdparamtable_pkey PRIMARY KEY (id);

ALTER TABLE ONLY typedefine
    ADD CONSTRAINT typedefine_pkey PRIMARY KEY (id);

ALTER TABLE ONLY userinfo
    ADD CONSTRAINT userinfo_pkey PRIMARY KEY (id);

ALTER TABLE ONLY vehiclebilltable
    ADD CONSTRAINT vehiclebilltable_pkey PRIMARY KEY (id);

ALTER TABLE ONLY vehiclecategoryinfo
    ADD CONSTRAINT vehiclecategoryinfo_pkey PRIMARY KEY (id);

ALTER TABLE ONLY vehicleinfo
    ADD CONSTRAINT vehicleinfo_pkey PRIMARY KEY (id);

ALTER TABLE ONLY vehiclepassruleinfo
    ADD CONSTRAINT vehiclepassruleinfo_pkey PRIMARY KEY (id);

ALTER TABLE ONLY vehicletypeinfo
    ADD CONSTRAINT vehicletypeinfo_pkey PRIMARY KEY (id);

ALTER TABLE ONLY worklog
    ADD CONSTRAINT worklog_pkey PRIMARY KEY (id);

CREATE INDEX alarmplateinfo_authority_idx ON alarmplateinfo USING btree (authority);
CREATE INDEX alarmplateinfo_contact_idx ON alarmplateinfo USING btree (contact);
CREATE INDEX alarmplateinfo_enableflag_idx ON alarmplateinfo USING btree (enableflag);
CREATE INDEX alarmplateinfo_plateno_gin_trgm_idx ON alarmplateinfo USING gin (plateno gin_trgm_ops);
CREATE INDEX alarmplateinfo_plateno_text_pattern_ops_idx ON alarmplateinfo USING btree (plateno text_pattern_ops);
CREATE INDEX alarmplateinfo_reason_idx ON alarmplateinfo USING btree (reason);
CREATE INDEX cardchargeinfo_timestamp_idx ON cardchargeinfo USING btree ("timestamp");
CREATE INDEX cardchargeinfo_type_idx ON cardchargeinfo USING btree (type);
CREATE INDEX cardchargeinfo_uniqueid_idx ON cardchargeinfo USING btree (uniqueid);
CREATE INDEX cardchargeinfo_uniqueno_idx ON cardchargeinfo USING btree (uniqueno);
CREATE INDEX cardchargeinfo_userid_idx ON cardchargeinfo USING btree (userid);
CREATE INDEX cardchargeinfo_username_idx ON cardchargeinfo USING btree (username);
CREATE INDEX cardcostinfo_centerid_idx ON cardcostinfo USING btree (centerid);
CREATE INDEX cardcostinfo_type_idx ON cardcostinfo USING btree (type);
CREATE INDEX cardfreetimeinfo_centerid_idx ON cardfreetimeinfo USING btree (centerid);
CREATE INDEX cardinfo_cardno_gin_trgm_idx ON cardinfo USING gin (cardno gin_trgm_ops);
CREATE INDEX cardinfo_cardno_text_pattern_ops_idx ON cardinfo USING btree (cardno text_pattern_ops);
CREATE INDEX cardinfo_chargetype_idx ON cardinfo USING btree (chargetype);
CREATE INDEX cardinfo_encryptcardno_idx ON cardinfo USING btree (encryptcardno);
CREATE INDEX cardinfo_endtime_idx ON cardinfo USING btree (endtime);
CREATE INDEX cardinfo_freetimeused_idx ON cardinfo USING btree (freetimeused);
CREATE INDEX cardinfo_uniquenumber_idx ON cardinfo USING btree (uniquenumber);
CREATE INDEX cardupdateinfo_uploadflag_idx ON cardupdateinfo USING btree (uploadflag);
CREATE INDEX categorypayrulemap_categoryid_idx ON categorypayrulemap USING btree (categoryid);
CREATE INDEX categorypayrulemap_centerid_idx ON categorypayrulemap USING btree (centerid);
CREATE INDEX categorypayrulemap_parkbelonged_idx ON categorypayrulemap USING btree (parkbelonged);
CREATE INDEX categorypayrulemap_payruleid_idx ON categorypayrulemap USING btree (payruleid);
CREATE INDEX charginginfo_cardno_gin_trgm_idx ON charginginfo USING gin (cardno gin_trgm_ops);
CREATE INDEX charginginfo_cardno_text_pattern_ops_idx ON charginginfo USING btree (cardno text_pattern_ops);
CREATE INDEX charginginfo_chargetype_idx ON charginginfo USING btree (chargetype);
CREATE INDEX charginginfo_checkserial_idx ON charginginfo USING btree (checkserial);
CREATE INDEX charginginfo_clienthash_idx ON charginginfo USING btree (clienthash);
CREATE INDEX charginginfo_gateno_idx ON charginginfo USING btree (gateno);
CREATE INDEX charginginfo_intime_idx ON charginginfo USING btree (intime);
CREATE INDEX charginginfo_inuniqueid_idx ON charginginfo USING btree (inuniqueid);
CREATE INDEX charginginfo_operatorid_idx ON charginginfo USING btree (operatorid);
CREATE INDEX charginginfo_outtime_idx ON charginginfo USING btree (outtime);
CREATE INDEX charginginfo_outuniqueid_idx ON charginginfo USING btree (outuniqueid);
CREATE INDEX charginginfo_parkbelonged_idx ON charginginfo USING btree (parkbelonged);
CREATE INDEX charginginfo_plateno_gin_trgm_idx ON charginginfo USING gin (plateno gin_trgm_ops);
CREATE INDEX charginginfo_plateno_text_pattern_ops_idx ON charginginfo USING btree (plateno text_pattern_ops);
CREATE INDEX charginginfo_uploadflag_idx ON charginginfo USING btree (uploadflag);
CREATE INDEX clientlist_clienthash_idx ON clientlist USING btree (clienthash);
CREATE INDEX clientlist_clientloginuser_idx ON clientlist USING btree (clientloginuser);
CREATE INDEX clientlist_updatetime_idx ON clientlist USING btree (updatetime);
CREATE INDEX controllercardinfo_cardno_text_pattern_ops_idx ON controllercardinfo USING btree (cardno text_pattern_ops);
CREATE INDEX controllercardinfo_clienthash_idx ON controllercardinfo USING btree (clienthash);
CREATE INDEX controllercardinfo_parkbelonged_idx ON controllercardinfo USING btree (parkbelonged);
CREATE INDEX currentturn_clienthash_idx ON currentturn USING btree (clienthash);
CREATE INDEX customdefine_name_idx ON customdefine USING btree (name);
CREATE INDEX customdefine_type_idx ON customdefine USING btree (type);
CREATE INDEX deductmoneytable_unid_idx ON deductmoneytable USING btree (unid);
CREATE INDEX deviceinfo_clienthash_idx ON deviceinfo USING btree (clienthash);
CREATE INDEX deviceinfo_devicetype_idx ON deviceinfo USING btree (devicetype);
CREATE INDEX devicemap_clienthash_idx ON devicemap USING btree (clienthash);
CREATE INDEX devicemap_relatedno_idx ON devicemap USING btree (relatedno);
CREATE INDEX etclisttable_license_idx ON etclisttable USING btree (license);
CREATE INDEX etcresulttable_billno_idx ON etcresulttable USING btree (billno);
CREATE INDEX etcresulttable_plateno_idx ON etcresulttable USING btree (plateno);
CREATE INDEX forcereleaseruleinfo_begintime_idx ON forcereleaseruleinfo USING btree (begintime);
CREATE INDEX forcereleaseruleinfo_centerid_idx ON forcereleaseruleinfo USING btree (centerid);
CREATE INDEX forcereleaseruleinfo_deleteflag_idx ON forcereleaseruleinfo USING btree (deleteflag);
CREATE INDEX forcereleaseruleinfo_endtime_idx ON forcereleaseruleinfo USING btree (endtime);
CREATE INDEX forcereleaseruleinfo_isdefault_idx ON forcereleaseruleinfo USING btree (isdefault);
CREATE INDEX forcereleaseruleinfo_selectable_idx ON forcereleaseruleinfo USING btree (selectable);
CREATE INDEX gateinfo_clienthash_idx ON gateinfo USING btree (clienthash);
CREATE INDEX gateinfo_gateno_idx ON gateinfo USING btree (gateno);
CREATE INDEX gateinfo_parkbelonged_idx ON gateinfo USING btree (parkbelonged);
CREATE INDEX holidayinfo_centerid_idx ON holidayinfo USING btree (centerid);
CREATE INDEX holidayinfo_date_idx ON holidayinfo USING btree (date);
CREATE INDEX holidays_specialdate_idx ON holidays USING btree (specialdate);
CREATE INDEX laneinfo_clienthash_idx ON laneinfo USING btree (clienthash);
CREATE INDEX laneinfo_laneno_idx ON laneinfo USING btree (laneno);
CREATE INDEX lastamoutpayable_inunid_idx ON lastamoutpayable USING btree (inunid);
CREATE INDEX lastgroupcarinfo_groupbelonged_idx ON lastgroupcarinfo USING btree (groupbelonged);
CREATE INDEX lastgroupcarinfo_plateno_idx ON lastgroupcarinfo USING btree (plateno);
CREATE INDEX leddisplayinfo_centerid_idx ON leddisplayinfo USING btree (centerid);
CREATE INDEX leddisplayinfo_clienthash_idx ON leddisplayinfo USING btree (clienthash);
CREATE INDEX leddisplayinfo_parkbelonged_idx ON leddisplayinfo USING btree (parkbelonged);
CREATE INDEX leddisplayinfo_type_idx ON leddisplayinfo USING btree (type);
CREATE INDEX localchargetable_inunid_idx ON localchargetable USING btree (inunid);
CREATE INDEX metainfo_metakey_idx ON metainfo USING btree (metakey);
CREATE INDEX operationrecordinfo_clienthash_idx ON operationrecordinfo USING btree (clienthash);
CREATE INDEX operationrecordinfo_operatorid_idx ON operationrecordinfo USING btree (operatorid);
CREATE INDEX operationrecordinfo_timestamp_idx ON operationrecordinfo USING btree ("timestamp");
CREATE INDEX operationrecordinfo_uploadflag_idx ON operationrecordinfo USING btree (uploadflag);
CREATE INDEX parkbindinginfotable_orderno_idx ON parkbindinginfotable USING btree (orderno);
CREATE INDEX parkbindinginfotable_plateno_idx ON parkbindinginfotable USING btree (plateno);
CREATE INDEX parkedvehicleinfo_belief_idx ON parkedvehicleinfo USING btree (belief);
CREATE INDEX parkedvehicleinfo_cardno_idx ON parkedvehicleinfo USING btree (cardno);
CREATE INDEX parkedvehicleinfo_cardtime_idx ON parkedvehicleinfo USING btree (cardtime);
CREATE INDEX parkedvehicleinfo_centerid_idx ON parkedvehicleinfo USING btree (centerid);
CREATE INDEX parkedvehicleinfo_clienthash_idx ON parkedvehicleinfo USING btree (clienthash);
CREATE INDEX parkedvehicleinfo_gateno_idx ON parkedvehicleinfo USING btree (gateno);
CREATE INDEX parkedvehicleinfo_laneno_idx ON parkedvehicleinfo USING btree (laneno);
CREATE INDEX parkedvehicleinfo_mainlogo_idx ON parkedvehicleinfo USING btree (mainlogo);
CREATE INDEX parkedvehicleinfo_parkbelonged_idx ON parkedvehicleinfo USING btree (parkbelonged);
CREATE INDEX parkedvehicleinfo_parkingtype_idx ON parkedvehicleinfo USING btree (parkingtype);
CREATE INDEX parkedvehicleinfo_passtime_idx ON parkedvehicleinfo USING btree (passtime);
CREATE INDEX parkedvehicleinfo_plateno_idx ON parkedvehicleinfo USING btree (plateno);
CREATE INDEX parkedvehicleinfo_refno_idx ON parkedvehicleinfo USING btree (refno);
CREATE INDEX parkedvehicleinfo_sublogo_idx ON parkedvehicleinfo USING btree (sublogo);
CREATE INDEX parkedvehiclepictable_clienthash_idx ON parkedvehiclepictable USING btree (clienthash);
CREATE INDEX parkedvehiclepictable_uniqueid_idx ON parkedvehiclepictable USING btree (uniqueid);
CREATE INDEX passfaceinfo_clienthash_idx ON passfaceinfo USING btree (clienthash);
CREATE INDEX passfaceinfo_direction_idx ON passfaceinfo USING btree (direction);
CREATE INDEX passfaceinfo_gateno_idx ON passfaceinfo USING btree (gateno);
CREATE INDEX passfaceinfo_laneno_idx ON passfaceinfo USING btree (laneno);
CREATE INDEX passfaceinfo_passtime_idx ON passfaceinfo USING btree (passtime);
CREATE INDEX passfaceinfo_uploadflag_idx ON passfaceinfo USING btree (uploadflag);
CREATE INDEX passvehicleinfo_belief_idx ON passvehicleinfo USING btree (belief);
CREATE INDEX passvehicleinfo_cardno_gin_trgm_idx ON passvehicleinfo USING gin (cardno gin_trgm_ops);
CREATE INDEX passvehicleinfo_cardno_text_pattern_ops_idx ON passvehicleinfo USING btree (cardno text_pattern_ops);
CREATE INDEX passvehicleinfo_chargetime_idx ON passvehicleinfo USING btree (chargetime);
CREATE INDEX passvehicleinfo_clienthash_idx ON passvehicleinfo USING btree (clienthash);
CREATE INDEX passvehicleinfo_handlestatus_idx ON passvehicleinfo USING btree (handlestatus) WHERE (handlestatus <> 2);
CREATE INDEX passvehicleinfo_mainlogo_idx ON passvehicleinfo USING btree (mainlogo);
CREATE INDEX passvehicleinfo_openuuid_idx ON passvehicleinfo USING btree (openuuid) WHERE (btrim((openuuid)::text) <> ''::text);
CREATE INDEX passvehicleinfo_operationtype_idx ON passvehicleinfo USING btree (operationtype);
CREATE INDEX passvehicleinfo_paireduniqueid_idx ON passvehicleinfo USING btree (paireduniqueid);
CREATE INDEX passvehicleinfo_passtime_idx ON passvehicleinfo USING btree (passtime);
CREATE INDEX passvehicleinfo_plateno_chargetime_idx ON passvehicleinfo USING btree (plateno, chargetime);
CREATE INDEX passvehicleinfo_plateno_gin_trgm_idx ON passvehicleinfo USING gin (plateno gin_trgm_ops);
CREATE INDEX passvehicleinfo_plateno_text_pattern_ops_idx ON passvehicleinfo USING btree (plateno text_pattern_ops);
CREATE INDEX passvehicleinfo_sublogo_idx ON passvehicleinfo USING btree (sublogo);
CREATE INDEX passvehicleinfo_uniqueid_idx ON passvehicleinfo USING btree (uniqueid);
CREATE INDEX passvehicleinfo_uploadflag_idx ON passvehicleinfo USING btree (uploadflag) WHERE (uploadflag = 0);
CREATE INDEX payruleinfo_deleteflag_idx ON payruleinfo USING btree (deleteflag);
CREATE INDEX reduceentry_operatorid_idx ON reduceentry USING btree (operatorid);
CREATE INDEX reduceentry_reduceruleid_idx ON reduceentry USING btree (reduceruleid);
CREATE INDEX reduceentry_status_idx ON reduceentry USING btree (status);
CREATE INDEX reduceentry_uuid_idx ON reduceentry USING btree (uuid);
CREATE INDEX reduceentry_validtime_idx ON reduceentry USING btree (validtime);
CREATE INDEX reduceruleinfo_begintime_idx ON reduceruleinfo USING btree (begintime);
CREATE INDEX reduceruleinfo_deleteflag_idx ON reduceruleinfo USING btree (deleteflag);
CREATE INDEX reduceruleinfo_endtime_idx ON reduceruleinfo USING btree (endtime);
CREATE INDEX shiftturnoverlog_begintime_idx ON shiftturnoverlog USING btree (begintime);
CREATE INDEX shiftturnoverlog_cardno_gin_trgm_idx ON shiftturnoverlog USING gin (cardno gin_trgm_ops);
CREATE INDEX shiftturnoverlog_cardno_text_pattern_ops_idx ON shiftturnoverlog USING btree (cardno text_pattern_ops);
CREATE INDEX shiftturnoverlog_clienthash_idx ON shiftturnoverlog USING btree (clienthash);
CREATE INDEX shiftturnoverlog_endtime_idx ON shiftturnoverlog USING btree (endtime);
CREATE INDEX shiftturnoverlog_operatorid_idx ON shiftturnoverlog USING btree (operatorid);
CREATE INDEX shiftturnoverlog_uploadflag_idx ON shiftturnoverlog USING btree (uploadflag);
CREATE INDEX spotsgroupinfo_groupname_idx ON spotsgroupinfo USING btree (groupname);
CREATE INDEX syncrecordinfo_operationtime_idx ON syncrecordinfo USING btree (operationtime);
CREATE INDEX syncrecordinfo_tablename_idx ON syncrecordinfo USING btree (tablename);
CREATE INDEX syncskipinfo_tableindex_idx ON syncskipinfo USING btree (tableindex);
CREATE INDEX syncskipinfo_tabletype_idx ON syncskipinfo USING btree (tabletype);
CREATE INDEX syncskipinfo_uploadflag_idx ON syncskipinfo USING btree (uploadflag);
CREATE INDEX thirdchargeinfo_billno_idx ON thirdchargeinfo USING btree (billno);
CREATE INDEX thirdchargeinfo_chargetime_idx ON thirdchargeinfo USING btree (chargetime DESC NULLS LAST);
CREATE INDEX thirdchargeinfo_haveused_idx ON thirdchargeinfo USING btree (haveused) WHERE (haveused = 0);
CREATE INDEX thirdchargeinfo_plateno_idx ON thirdchargeinfo USING btree (plateno DESC NULLS LAST);
CREATE INDEX userinfo_autologinflag_idx ON userinfo USING btree (autologinflag);
CREATE INDEX vehiclebilltable_billcode_idx ON vehiclebilltable USING btree (billcode);
CREATE INDEX vehiclebilltable_unid_idx ON vehiclebilltable USING btree (unid);
CREATE INDEX vehiclecategoryinfo_centerid_idx ON vehiclecategoryinfo USING btree (centerid);
CREATE INDEX vehiclecategoryinfo_type_idx ON vehiclecategoryinfo USING btree (type);
CREATE INDEX vehicleinfo_cardno_gin_trgm_idx ON vehicleinfo USING gin (cardno gin_trgm_ops);
CREATE INDEX vehicleinfo_cardno_text_pattern_ops_idx ON vehicleinfo USING btree (cardno text_pattern_ops);
CREATE INDEX vehicleinfo_categorybelonged_idx ON vehicleinfo USING btree (categorybelonged);
CREATE INDEX vehicleinfo_ownername_idx ON vehicleinfo USING btree (ownername);
CREATE INDEX vehicleinfo_parkingtype_idx ON vehicleinfo USING btree (parkingtype);
CREATE INDEX vehicleinfo_platecolor_idx ON vehicleinfo USING btree (platecolor);
CREATE INDEX vehicleinfo_plateno_gin_trgm_idx ON vehicleinfo USING gin (plateno gin_trgm_ops);
CREATE INDEX vehicleinfo_plateno_text_pattern_ops_idx ON vehicleinfo USING btree (plateno text_pattern_ops);
CREATE INDEX vehicleinfo_uniquenumber_idx ON vehicleinfo USING btree (uniquenumber);
CREATE INDEX vehicleinfo_vehiclecolor_idx ON vehicleinfo USING btree (vehiclecolor);
CREATE INDEX vehicleinfo_vehicletype_idx ON vehicleinfo USING btree (vehicletype);
