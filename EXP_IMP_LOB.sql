
create or replace package EXP_IMP_LOB is

/* *******************************************************************************************************

    History of changes
    yyyy.mm.dd | Version | Author         | Changes
    -----------+---------+----------------+-------------------------
    2017.01.06 |  1.0    | Ferenc Toth    | Created 

******************************************************************************************************* */


  type T_STRING_LIST is table of varchar2( 32000 );

    ---------------------------------------------------------------------------
    function  EXPORT ( I_TABLE_NAME  in varchar2
                     , I_COLUMN_NAME in varchar2
                     , I_WHERE       in varchar2 default null
                     ) return T_STRING_LIST pipelined;
    ---------------------------------------------------------------------------
    
    ---------------------------------------------------------------------------
    procedure IMPORT_NEW;
    ---------------------------------------------------------------------------

    ---------------------------------------------------------------------------
    procedure IMPORT_APPEND ( I_RAW         in varchar2);
    ---------------------------------------------------------------------------

    ---------------------------------------------------------------------------
    procedure DIRECT_SQL ( I_SQL  in varchar2 );
    ---------------------------------------------------------------------------

    ---------------------------------------------------------------------------
    procedure IMPORT_UPDATE ( I_TABLE_NAME  in varchar2
                            , I_COLUMN_NAME in varchar2
                            , I_PK          in varchar2
                            );
    ---------------------------------------------------------------------------

end;
/



create or replace package body EXP_IMP_LOB is

/* *******************************************************************************************************

    History of changes
    yyyy.mm.dd | Version | Author         | Changes
    -----------+---------+----------------+-------------------------
    2017.01.06 |  1.0    | Ferenc Toth    | Created 

******************************************************************************************************* */

    G_TABLE_NAME    varchar(   40 );
    G_COLUMN_NAME   varchar(   40 );
    G_COLUMN_TYPE   varchar(   40 );
    G_PK_KEY        varchar( 4000 );
    G_PK_LST        varchar( 4000 );
    G_LENGTH        number := 200;
    G_BLOB          blob;
    G_CLOB          clob;

    ---------------------------------------------------------------------------
    procedure GET_PK ( I_TABLE_NAME in varchar ) is
    ---------------------------------------------------------------------------
        L_SEP           varchar ( 40 ) := ',';
        L_DATA_TYPE     varchar2( 30 );
    begin
        G_PK_KEY := '';
        G_PK_LST := '';
        for L_A_PK in ( select COLUMN_NAME
                          from USER_CONSTRAINTS UC
                             , USER_CONS_COLUMNS DBC
                         where UC.CONSTRAINT_TYPE  = 'P'
                           and DBC.CONSTRAINT_NAME = UC.CONSTRAINT_NAME
                           and DBC.TABLE_NAME      = I_TABLE_NAME 
                         order by position 
                      ) 
        loop
            if nvl( length( G_PK_KEY ), 0 ) + length( L_A_PK.COLUMN_NAME ) < 4000 then
                select DATA_TYPE into L_DATA_TYPE from user_tab_columns where table_name = G_TABLE_NAME and column_name = L_A_PK.COLUMN_NAME;
                if instr( L_DATA_TYPE, 'CHAR') > 0 then
                    G_PK_KEY := G_PK_KEY||'''''''''||'||L_A_PK.COLUMN_NAME||'||''''''''||'''||L_SEP||'''||';
                elsif instr( L_DATA_TYPE, 'DATE') > 0 then
                    G_PK_KEY := G_PK_KEY||'''TO_DATE(''''''||TO_CHAR('||L_A_PK.COLUMN_NAME||',''YYYY.MM.DD HH24:MI:SS'')||'''''',''''YYYY.MM.DD HH24:MI:SS'''')''||'''||L_SEP||'''||';
                else
                    G_PK_KEY := G_PK_KEY||L_A_PK.COLUMN_NAME||'||'''||L_SEP||'''||';
                end if;
                G_PK_LST := G_PK_LST||L_A_PK.COLUMN_NAME||L_SEP;
            end if;
        end loop;
        G_PK_KEY := substr( G_PK_KEY, 1, length( G_PK_KEY ) - ( 6 + length( L_SEP ) ) );
        G_PK_LST := substr( G_PK_LST, 1, length( G_PK_LST ) - length(L_SEP));
    end;


    ---------------------------------------------------------------------------
    function EXPORT ( I_TABLE_NAME  in varchar2
                    , I_COLUMN_NAME in varchar2
                    , I_WHERE       in varchar2 default null
                    ) return T_STRING_LIST pipelined is
    ---------------------------------------------------------------------------
        V_BLOB          blob;
        V_CLOB          clob;
        V_CUR_SQL       varchar( 32000 );
        V_LOB_SQL       varchar( 32000 );
        V_RAW           varchar( 32000 );
        V_START         number;
        V_PK            varchar(  4000 );
        V_REC_SET       sys_refcursor; 

    begin
        G_TABLE_NAME  := upper( trim( I_TABLE_NAME  ) );
        G_COLUMN_NAME := upper( trim( I_COLUMN_NAME ) );
        GET_PK( G_TABLE_NAME );
        select DATA_TYPE into G_COLUMN_TYPE from user_tab_columns where table_name = G_TABLE_NAME and column_name = G_COLUMN_NAME;
        if G_COLUMN_TYPE not in ('CLOB','NCLOB','BLOB') then
            raise_application_error ( -20001, 'The type of column '||I_COLUMN_NAME||' is not CLOB, NCLOB or BLOB' );    
        end if;

        V_CUR_SQL := 'select '||G_PK_KEY||' from '||G_TABLE_NAME||' where '||nvl( I_WHERE, ' 1 = 1 ');
        open V_REC_SET for V_CUR_SQL;
        loop
            fetch V_REC_SET into V_PK;
            exit when V_REC_SET%notfound; 
            PIPE ROW( '/******************************************************' );
            PIPE ROW( '   TABLE  :'||G_TABLE_NAME                               );
            PIPE ROW( '   COLUMN :'||G_COLUMN_NAME                              );
            PIPE ROW( '   ROW    :'||V_PK                                       );
            PIPE ROW( '******************************************************/' );
            PIPE ROW( 'BEGIN'                                                   );
            PIPE ROW( '   EXP_IMP_LOB.IMPORT_NEW;'                              );
            V_LOB_SQL := 'select '||G_COLUMN_NAME||' from '||G_TABLE_NAME||' where ('||G_PK_LST||') in ( select '||V_PK||' from dual )';

            if G_COLUMN_TYPE = 'BLOB' then
                execute immediate V_LOB_SQL into V_BLOB;
                if nvl( dbms_lob.getlength( V_BLOB ), 0 ) > 0 then
                    V_START := 1;
                    for L_I IN 1..ceil( dbms_lob.getlength( V_BLOB ) / G_LENGTH )
                    loop
                        V_RAW   := dbms_lob.substr( V_BLOB, G_LENGTH, V_START );
                        PIPE ROW( '   EXP_IMP_LOB.IMPORT_APPEND ( '''||V_RAW||''');'         );
                        V_START := V_START + G_LENGTH;
                    end loop;
                    PIPE ROW( '   EXP_IMP_LOB.IMPORT_UPDATE ( '''||G_TABLE_NAME||''','''||G_COLUMN_NAME||''','''||replace(V_PK,'''','''''')||''' ); ');
                    PIPE ROW( '   COMMIT;'                                              );
                end if;
            else
                execute immediate V_LOB_SQL into V_CLOB;
                if nvl( dbms_lob.getlength( V_CLOB ), 0 ) > 0 then
                    V_START := 1;
                    for L_I IN 1..ceil( dbms_lob.getlength( V_CLOB ) / G_LENGTH )
                    loop
                        V_RAW   := UTL_RAW.CAST_TO_RAW( dbms_lob.substr( V_CLOB, G_LENGTH, V_START ) );
                        PIPE ROW( '   EXP_IMP_LOB.IMPORT_APPEND ( '''||V_RAW||''');'         );
                        V_START := V_START + G_LENGTH;
                    end loop;
                    PIPE ROW( '   EXP_IMP_LOB.IMPORT_UPDATE ( '''||G_TABLE_NAME||''','''||G_COLUMN_NAME||''','''||replace(V_PK,'''','''''')||''' ); ');
                    PIPE ROW( '   COMMIT;'                                              );
                end if;
            end if;
            PIPE ROW( 'END;'                                                    );   
            PIPE ROW( '/'                                                       );
            PIPE ROW( ' '                                                       );
        end loop;
        close V_REC_SET;

        return;

    end;

    ---------------------------------------------------------------------------
    procedure IMPORT_NEW is
    ---------------------------------------------------------------------------
    begin
        G_BLOB := null;
        G_CLOB := null;
    end;

    ---------------------------------------------------------------------------
    procedure IMPORT_APPEND ( I_RAW         in varchar2 ) is
    ---------------------------------------------------------------------------
        V_BLOB          blob;
    begin
        V_BLOB := hextoraw( I_RAW );
        if nvl( dbms_lob.getlength( V_BLOB ), 0 ) > 0 then
            if nvl( dbms_lob.getlength( G_BLOB ), 0 ) = 0 then 
                G_BLOB := V_BLOB;
            else
                DBMS_LOB.APPEND( G_BLOB, V_BLOB );
            end if;
        end if;       
    end;

    ---------------------------------------------------------------------------
    procedure DIRECT_SQL ( I_SQL  in varchar2 ) is
    ---------------------------------------------------------------------------
    begin
        if nvl( dbms_lob.getlength( G_BLOB ), 0 ) > 0 then
            execute immediate I_SQL using G_BLOB;
        else
            execute immediate I_SQL using G_CLOB;
        end if;
        commit;
    end;

    -- I downloaded this from the Net:
    function clobfromblob( p_blob blob ) return clob is
        l_clob         clob;
        l_dest_offsset integer := 1;
        l_src_offsset  integer := 1;
        l_lang_context integer := dbms_lob.default_lang_ctx;
        l_warning      integer;
    begin
        if p_blob is null then
            return null;
        end if;
        dbms_lob.createTemporary(lob_loc => l_clob
                                ,cache   => false);
        dbms_lob.converttoclob(dest_lob     => l_clob
                              ,src_blob     => p_blob
                              ,amount       => dbms_lob.lobmaxsize
                              ,dest_offset  => l_dest_offsset
                              ,src_offset   => l_src_offsset
                              ,blob_csid    => dbms_lob.default_csid
                              ,lang_context => l_lang_context
                              ,warning      => l_warning);
        return l_clob;
    end;


    ---------------------------------------------------------------------------
    procedure IMPORT_UPDATE ( I_TABLE_NAME  in varchar2
                            , I_COLUMN_NAME in varchar2
                            , I_PK          in varchar2
                            ) is
    ---------------------------------------------------------------------------
        V_SQL           varchar( 32000 );
    begin
        G_TABLE_NAME  := upper( trim( I_TABLE_NAME  ) );
        G_COLUMN_NAME := upper( trim( I_COLUMN_NAME ) );
        GET_PK( G_TABLE_NAME );
        select DATA_TYPE into G_COLUMN_TYPE from user_tab_columns where table_name = G_TABLE_NAME and column_name = G_COLUMN_NAME;
        V_SQL := 'update '||I_TABLE_NAME||' set '||I_COLUMN_NAME||' = :1 where ('||G_PK_LST||') in ( select '||I_PK||' from dual )';
        if G_COLUMN_TYPE in ( 'CLOB', 'NCLOB' ) then
            G_CLOB := clobfromblob ( G_BLOB );
            G_BLOB := null;
            DIRECT_SQL( V_SQL );
        elsif G_COLUMN_TYPE in ( 'BLOB' ) then
            DIRECT_SQL( V_SQL );
        end if;
    end;


end;
/