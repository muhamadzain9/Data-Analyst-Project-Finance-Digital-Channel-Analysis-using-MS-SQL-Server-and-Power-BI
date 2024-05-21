DECLARE @CurrentDatebefore DATE = GETDATE()-1;
WITH CTE AS (
    SELECT ABCD.*,
        ROW_NUMBER() OVER (PARTITION BY ABCD.agreementno ORDER BY ABCD.max__pt) AS rownum
    FROM (
        SELECT M.*,
            CASE 
                WHEN DPD_Max_1 = 0 THEN 1
                WHEN DPD_Max_1 IS NOT NULL THEN 0
                ELSE 1
            END AS SR
        FROM (
            SELECT
                Q.agreementno,
                Q.product,
                Q.angsuran,
                Q.min__pt,
                Q.max__pt,
                P.DaysOverDueTolerance AS DPD_Min,
                R.DaysOverDueTolerance AS DPD_Max,
                W.DaysOverDueTolerance AS DPD_Max_1,
                P.TotalOSPrincipal,
                P.[Installment Amount Latest],
                P.DefaultStatus,
                Y.Provinsi,
                Y.Kab,
                Y.Kec,
                Y.Desa
            FROM (
                SELECT
                    X.agreementno,
                    X.product,
                    X.angsuran,
                    MIN(X.pt) min__pt,
                    MAX(X.pt) max__pt
                FROM (
                    SELECT Base.*,
                        inst.angsuran,
                        inst.DefaultStatus
                    FROM (
                        SELECT  
                            agreementno,
                            product,
                            assigned_dpd,
                            pt,
                            subcgid,
                            disposition
                        FROM DB_Bravo.dbo.activity
                        WHERE pt BETWEEN DATEFROMPARTS(YEAR(@CurrentDatebefore), MONTH(@CurrentDatebefore) , 1) AND GETDATE()-1
                        AND agent_type IN ('WHATSAPP', 'IVR', 'SMS', 'DIGITAL')
                    ) Base
                    LEFT JOIN (
                        SELECT angsuran, AgreementNo, ReportDate, DefaultStatus
                        FROM [Collection].dbo.list
                        WHERE ReportDate BETWEEN EOMONTH(@CurrentDatebefore, -1) AND GETDATE()-1
                    ) Inst ON Inst.AgreementNo = Base.agreementno AND DATEADD(DAY, -1, CONVERT(VARCHAR, base.pt, 112)) = CONVERT(VARCHAR, Inst.ReportDate, 112)
                ) X
                WHERE X.assigned_dpd <> '0' and left(X.assigned_dpd,1) <> '-' AND X.DefaultStatus = 'NM' AND X.product IN ('CAR', 'MCY')
                GROUP BY X.agreementno, X.product, X.angsuran
            ) Q
            LEFT JOIN ( 
                SELECT 
                    AgreementNo,
                    DaysOverDueTolerance,
                    ReportDate,
                    TotalOSPrincipal,
                    [Installment Amount Latest],
                    DefaultStatus
                FROM [Collection].dbo.list
                WHERE ReportDate BETWEEN EOMONTH(@CurrentDatebefore, -1) AND GETDATE()-1
            ) P ON TRIM(Q.agreementno) = TRIM(P.AgreementNo) AND DATEADD(DAY, -1, CONVERT(VARCHAR, Q.min__pt, 112)) = CONVERT(VARCHAR, P.ReportDate, 112)
            LEFT JOIN (
                SELECT 
                    AgreementNo,
                    DaysOverDueTolerance,
                    ReportDate
                FROM [Collection].dbo.list
                WHERE ReportDate BETWEEN EOMONTH(@CurrentDatebefore, -1) AND GETDATE()-1
            ) R ON TRIM(Q.agreementno) = TRIM(R.AgreementNo) AND DATEADD(DAY, -1, CONVERT(VARCHAR, Q.max__pt, 112)) = CONVERT(VARCHAR, R.ReportDate, 112)
            LEFT JOIN (
                SELECT 
                    AgreementNo,
                    DaysOverDueTolerance,
                    ReportDate
                FROM [Collection].dbo.list
                WHERE ReportDate BETWEEN EOMONTH(@CurrentDatebefore, -1) AND GETDATE()-1
            ) W ON TRIM(Q.agreementno) = TRIM(W.agreementno) AND CONVERT(VARCHAR, Q.max__pt, 112) = CONVERT(VARCHAR, W.ReportDate, 112)
            LEFT JOIN (
                SELECT Provinsi, Desa, AgreementNo, Kab, SubCGID, Kec
                FROM [Collection].dbo.provinsi WITH(NOLOCK)
                WHERE SK_Time = CONVERT(VARCHAR(8), EOMONTH(@CurrentDatebefore), 112)
            ) Y ON TRIM(Q.agreementno) = TRIM(Y.agreementno)
        ) M
    ) ABCD
),
CTEE AS (
    SELECT *,
        CONCAT(CTE.agreementno, CTE.angsuran) AS Dummy,
  DATEADD(DAY, 1, CONVERT(VARCHAR, max__pt, 112)) as max__pt_1
    FROM CTE
    LEFT JOIN (
        SELECT 
            AgreementNo as nomor_kontrak,
   TotalOSPrincipal as osp,
            DaysOverDueTolerance as DPD_Max_2, 
            [Installment Amount Latest] as installment_amount,
            ReportDate
        FROM [Collection].dbo.list
        WHERE ReportDate BETWEEN EOMONTH(@CurrentDatebefore, -1) AND GETDATE()-1 
    ) bb ON TRIM(CTE.agreementno) = TRIM(bb.nomor_kontrak) AND DATEADD(DAY, 1, CONVERT(VARCHAR, max__pt, 112)) = CONVERT(VARCHAR, bb.ReportDate, 112)
),

filtered_duplicate_yuris AS (
  SELECT
    agreementno, agent_type, pt,
    ROW_NUMBER() OVER(PARTITION BY agreementno, pt order by agent_type) AS row_number
  FROM DB_Bravo.dbo.activity
  where pt between DATEFROMPARTS(YEAR(@CurrentDatebefore), MONTH(@CurrentDatebefore) , 1) AND GETDATE()-1
),

CTEEE AS (
SELECT *
FROM CTEE
LEFT JOIN (
 SELECT agreementno as no_kontrak,
  agent_type,
  pt
 from filtered_duplicate_yuris
 where row_number = 1 ) cc
 ON TRIM(CTEE.agreementno) = TRIM(cc.no_kontrak) AND DATEADD(DAY, 1, CONVERT(VARCHAR, max__pt, 112)) = CONVERT(VARCHAR, cc.pt, 112)),

 CTEEEE AS (
 SELECT *
 FROM CTEEE
 WHERE SR = 0 AND max__pt_1 <> CONVERT(DATETIME, CONVERT(DATE, GETDATE()))
 ),

 newSR AS (
 SELECT *,
 CASE
 WHEN SR = 0 and  DPD_Max_2 = 0 and agent_type is null THEN 1
 WHEN SR = 0 and DPD_Max_2 is null and agent_type is null THEN 1
ELSE 0 END AS newSR
 FROM CTEEEE),

 SR0 AS (
 SELECT 
 agreementno,
 product,
 angsuran,
 min__pt,
 max__pt,
 DPD_Min,
 DPD_Max,
 DPD_Max_1,
 TotalOSPrincipal,
 [Installment Amount Latest],
 DefaultStatus,
 Provinsi,
 Kab,
 Kec,
 Desa,
 SR,
 rownum,
 nomor_kontrak,
 osp,
 DPD_Max_2,
 installment_amount,
 ReportDate,
 Region,
 Dummy,
 max__pt_1,
 no_kontrak,
 agent_type,
 pt,
 newSR
FROM newSR),

SR01 AS (
SELECT *,
CASE
 WHEN SR = SR THEN SR
 END AS newSR
 FROM CTEEE
 WHERE SR = 0 AND max__pt_1 = CONVERT(DATETIME, CONVERT(DATE, GETDATE()))
 ),

 SR1 AS (
 SELECT *,
 CASE
 WHEN SR = SR THEN SR
 END AS newSR
 FROM CTEEE
 WHERE SR = 1
 ), 

 --UNIK CTR INSTALLMENT
UNIK_CTR_INSTALLMENT AS(
 SELECT * FROM SR0
 UNION ALL
 SELECT * FROM SR01
 UNION ALL 
 SELECT * FROM SR1
),

--UNIK CTR
UNIK_CTR AS (
 SELECT *,
 ROW_NUMBER() OVER (PARTITION BY agreementno ORDER BY agreementno asc, newSR desc) AS rn
 FROM UNIK_CTR_INSTALLMENT
),

-- DIGITAL BLAST (UNIK CTR INSTALLMENT)
BLAST AS (
Select ABCD.*,
 ROW_NUMBER() OVER (PARTITION BY ABCD.agreementno order by ABCD.max__pt) as rownum
From(
Select M.*,
    case 
 when DPD_Max_1 = 0 then 1
 when DPD_Max_1 is not null then 0
    else 1
    end AS SR
from(
select
    Q.agreementno,
    Q.product, 
    Q.angsuran,
    Q.min__pt,
    Q.max__pt, 
    Q.Grouped_Disposition,
    P.DaysOverDueTolerance as DPD_Min,
    R.DaysOverDueTolerance as DPD_Max, 
    W.DaysOverDueTolerance as DPD_Max_1,
    P.TotalOSPrincipal,
    P.[Installment Amount Latest],
    P.DefaultStatus,
    Y.Provinsi,
    Y.Kab,
    Y.Kec,
    Y.Desa
  
from (
    select 
        X.agreementno,
        X.product,
        X.angsuran,
        min(X.pt) min__pt, 
        max(X.pt) max__pt,
        X.Grouped_Disposition
    from (
        select Base.*, inst.angsuran, inst.DefaultStatus from (select  
            agreementno, 
            product, 
            assigned_dpd,
            pt,
            subcgid,
            CASE 
            WHEN disposition IN ('DELIVERED','READ','REPLIED') AND agent_type = 'WHATSAPP' THEN 'CONTACTED'
            WHEN disposition IN ('ANSWERED') AND agent_type = 'IVR' THEN 'CONTACTED'
            WHEN disposition IN ('SENT','DELIVERED') AND agent_type = 'SMS' THEN 'CONTACTED'
            ELSE 'UNCONTACTED'
            END AS Grouped_Disposition
        from DB_Bravo.dbo.activity WHERE pt between DATEFROMPARTS(YEAR(@CurrentDatebefore), MONTH(@CurrentDatebefore) , 1) AND GETDATE()-1
  and agent_type IN ('WHATSAPP','IVR','SMS')) Base
  left join (
   select angsuran, AgreementNo, ReportDate, DefaultStatus
   from [Collection].dbo.list where ReportDate between EOMONTH(@CurrentDatebefore, -1) AND GETDATE()-1 
   ) Inst on Inst.AgreementNo = Base.agreementno and dateadd(day,-1,convert(varchar,base.pt,112)) = convert(varchar,Inst.ReportDate,112)
    ) X
    WHERE X.assigned_dpd <> '0' and left(X.assigned_dpd,1) <> '-' AND X.DefaultStatus = 'NM' AND X.product IN ('CAR', 'MCY')
    group by
        X.agreementno, 
        X.product,
        X.angsuran,
        X.Grouped_Disposition
) Q

left join ( 
    select 
        AgreementNo,
        Product, 
        DaysOverDueTolerance,
        ReportDate,
        TotalOSPrincipal,
        [Installment Amount Latest],
        DefaultStatus
    from [Collection].dbo.list WHERE ReportDate between EOMONTH(@CurrentDatebefore, -1) AND GETDATE()-1
    
) P on trim(Q.agreementno) = trim(P.AgreementNo) and dateadd(day,-1,convert(varchar,Q.min__pt,112)) = convert(varchar,P.ReportDate,112)
left join (
    select 
        AgreementNo,
        Product, 
        DaysOverDueTolerance,
        ReportDate
    from [Collection].dbo.list WHERE ReportDate between EOMONTH(@CurrentDatebefore, -1) AND GETDATE()-1
   
) R on trim(Q.agreementno) = trim(R.AgreementNo) and dateadd(day,-1,convert(varchar,Q.max__pt,112)) = convert(varchar,R.ReportDate,112)
left join (   
    select 
        AgreementNo,
        Product, 
        DaysOverDueTolerance,
        ReportDate
    from [Collection].dbo.list WHERE ReportDate between EOMONTH(@CurrentDatebefore, -1) AND GETDATE()-1

) W on trim(Q.agreementno) = trim(W.agreementno) and convert(varchar,Q.max__pt,112) = convert(varchar,W.ReportDate,112)
left join (
 select Provinsi,Desa,AgreementNo,Kab,SubCGID,Kec
 FROM [Collection].dbo.provinsi with(nolock)
 where SK_Time = CONVERT(VARCHAR(8), EOMONTH(@CurrentDatebefore), 112)) Y
 ON TRIM(q.agreementno) = TRIM(Y.agreementno)

) M
) ABCD
),

BLAST1 AS (
SELECT
 b.agreementno,
 b.product,
 b.angsuran,
 b.min__pt,
 b.max__pt,
 b.Grouped_Disposition,
 b.DPD_Min,
 b.DPD_Max,
 b.DPD_Max_1,
 b.TotalOSPrincipal,
 b.[Installment Amount Latest],
 b.DefaultStatus,
 b.Provinsi,
 b.Kab,
 b.Kec,
 b.Desa,
 b.SR,
 b.rownum,
 a.newSR,
 a.max__pt_1,
 a.DPD_Max_2,
 a.agent_type
FROM BLAST b
LEFT JOIN (
select
 agreementno,
 angsuran,
 newSR,
 max__pt_1,
 DPD_Max_2,
 agent_type
from UNIK_CTR_INSTALLMENT) a
ON a.agreementno = b.agreementno and a.angsuran = b.angsuran
),

BLAST2 AS (
SELECT *
 ROW_NUMBER () OVER (PARTITION BY agreementno, angsuran ORDER BY Grouped_Disposition asc) as row_num
FROM BLAST1
),

--SELECT *
--FROM BLAST2
--WHERE row_num = 1

--FUNNEL MEDIA
FUNNEL AS (
SELECT TRIM(base.agreementno) AS agreementno
  ,base.assigned_dpd
  ,base.assigned_date
  ,base.activity_date
  ,base.agent_id
  ,base.agent_type
  ,base.product
  ,base.disposition
  ,contact_number
  ,CASE
     WHEN disposition IN ('DELIVERED','READ','REPLIED') AND agent_type = 'WHATSAPP' THEN 'CONTACTED'
     WHEN disposition = 'ANSWERED' AND agent_type = 'IVR' THEN 'CONTACTED'
     WHEN disposition IN ('SENT','DELIVERED') AND agent_type = 'SMS' THEN 'CONTACTED'
     ELSE 'UNCONTACTED'
   END AS Grouped_Disposition
  ,persona_name
  ,contact_type
  ,pt
  ,b.Provinsi
  ,b.Desa
  ,b.Kab
  ,b.Kec
  ,x.angsuran
FROM [DB_Bravo].[dbo].[activity] base

LEFT JOIN (
SELECT * FROM Collection.dbo.provinsi WITH(NOLOCK)
WHERE SK_Time = CONVERT(VARCHAR(8), EOMONTH(@CurrentDatebefore), 112)) b
on trim(base.agreementno) = trim(b.agreementno)

LEFT JOIN (
select angsuran, AgreementNo, ReportDate, DefaultStatus
from [Collection].dbo.list where ReportDate between EOMONTH(@CurrentDatebefore, -1) AND GETDATE()-1   --and DefaultStatus <> 'WO'
) X on TRIM(base.agreementno) = TRIM(X.AgreementNo) and dateadd(day,-1,convert(varchar,base.pt, 112)) = convert(varchar, X.ReportDate, 112)

WHERE base.product IN ('CAR','MCY')
AND base.pt BETWEEN DATEFROMPARTS(YEAR(@CurrentDatebefore), MONTH(@CurrentDatebefore) , 1) AND GETDATE()-1
AND base.agent_type IN ('WHATSAPP','IVR','SMS')
AND x.DefaultStatus <> 'WO'
),

FUNNEL1 AS (
 SELECT 
 b.agreementno,
 b.assigned_dpd,
 b.activity_date,
 b.agent_id,
 b.agent_type,
 b.product,
 b.disposition,
 b.contact_number,
 b.Grouped_Disposition,
 b.persona_name,
 b.contact_type,
 b.pt,
 b.Provinsi,
 b.Desa,
 b.Kab,
 b.Kec,
 b.angsuran,
 a.newSR,
 a.TotalOSPrincipal,
 a.max__pt_1,
 a.DPD_Max_2,
CASE
 WHEN b.agent_type = 'WHATSAPP' and b.disposition = 'DELIVERED' THEN 'DELIVERED'
 WHEN b.agent_type = 'WHATSAPP' and b.disposition = 'READ' THEN 'READ'
 WHEN b.agent_type = 'WHATSAPP' and b.disposition = 'REPLIED' THEN 'REPLIED'
 WHEN b.agent_type = 'WHATSAPP' and b.disposition = 'FAILED' THEN 'FAILED'
 WHEN b.agent_type = 'WHATSAPP' and b.disposition = 'SCHEDULED' THEN 'FAILED'
 WHEN b.agent_type = 'WHATSAPP' and b.disposition = 'TRIGGERED' THEN 'FAILED'
 WHEN b.agent_type = 'WHATSAPP' and b.disposition = 'SENT' THEN 'SENT'
 WHEN b.agent_type = 'IVR' and b.disposition = 'ANSWERED' THEN 'ANSWERED'
 WHEN b.agent_type = 'IVR' and b.disposition = 'NOT_ANSWERED' THEN 'NOT_ANSWERED'
 WHEN b.agent_type = 'IVR' and b.disposition = 'FAILED' THEN 'FAILED'
 WHEN b.agent_type = 'SMS' and b.disposition = 'SENT' THEN 'SENT'
 WHEN b.agent_type = 'SMS' and b.disposition = 'DELIVERED' THEN 'DELIVERED'
 WHEN b.agent_type = 'SMS' and b.disposition = 'REJECTED' THEN 'NOT SENT'
 WHEN b.agent_type = 'SMS' and b.disposition = 'UNDELIV' THEN 'NOT SENT'
 WHEN b.agent_type = 'SMS' and b.disposition = 'EXPIRED' THEN 'NOT SENT'
 WHEN b.agent_type = 'SMS' and b.disposition = 'UNKNOWN' THEN 'NOT SENT'
 WHEN b.agent_type = 'SMS' and b.disposition = 'BUFFERED' THEN 'NOT SENT'
 WHEN b.agent_type = 'SMS' and b.disposition = 'UNDELIVRD' THEN 'NOT SENT'
 WHEN b.agent_type = 'SMS' and b.disposition = 'REJECTD' THEN 'NOT SENT'
 WHEN b.agent_type = 'SMS' and b.disposition = 'UNDELIVERD' THEN 'NOT SENT'
 WHEN b.agent_type = 'SMS' and b.disposition = 'FAILED' THEN 'NOT SENT'
END AS Disposition_NEW
FROM FUNNEL b
LEFT JOIN (
select
 agreementno,
 TotalOSPrincipal,
 angsuran,
 newSR,
 max__pt_1,
 DPD_Max_2,
 agent_type
from UNIK_CTR_INSTALLMENT) a
ON a.agreementno = b.agreementno and a.angsuran = b.angsuran
),

FUNNEL2 AS (
SELECT *,
CASE
 WHEN agent_type = 'WHATSAPP' and Disposition_NEW = 'DELIVERED' THEN 'Bahan WA'
 WHEN agent_type = 'WHATSAPP' and Disposition_NEW = 'FAILED' THEN 'Bahan WA'
 WHEN agent_type = 'WHATSAPP' and Disposition_NEW = 'READ' THEN 'Bahan WA'
 WHEN agent_type = 'WHATSAPP' and Disposition_NEW = 'REPLIED' THEN 'Bahan WA'
 WHEN agent_type = 'WHATSAPP' and Disposition_NEW = 'SENT' THEN 'Bahan WA'
END AS Disposition_BahanWA,
CASE
 WHEN agent_type = 'WHATSAPP' and Disposition_NEW = 'DELIVERED' THEN 'Sent'
 WHEN agent_type = 'WHATSAPP' and Disposition_NEW = 'FAILED' THEN 'Not Sent'
 WHEN agent_type = 'WHATSAPP' and Disposition_NEW = 'READ' THEN 'Sent'
 WHEN agent_type = 'WHATSAPP' and Disposition_NEW = 'REPLIED' THEN 'Sent'
 WHEN agent_type = 'WHATSAPP' and Disposition_NEW = 'SENT' THEN 'Sent'
END AS Disposition_SentWA,
CASE
 WHEN agent_type = 'WHATSAPP' and Disposition_NEW = 'DELIVERED' THEN 'Delivered'
 WHEN agent_type = 'WHATSAPP' and Disposition_NEW = 'FAILED' THEN 'Not Delivered'
 WHEN agent_type = 'WHATSAPP' and Disposition_NEW = 'READ' THEN 'Delivered'
 WHEN agent_type = 'WHATSAPP' and Disposition_NEW = 'REPLIED' THEN 'Delivered'
 WHEN agent_type = 'WHATSAPP' and Disposition_NEW = 'SENT' THEN 'Not Delivered'
END AS Disposition_DeliveredWA,
CASE
 WHEN agent_type = 'WHATSAPP' and Disposition_NEW = 'DELIVERED' THEN 'Not Read'
 WHEN agent_type = 'WHATSAPP' and Disposition_NEW = 'FAILED' THEN 'Not Read'
 WHEN agent_type = 'WHATSAPP' and Disposition_NEW = 'READ' THEN 'Read'
 WHEN agent_type = 'WHATSAPP' and Disposition_NEW = 'REPLIED' THEN 'Read'
 WHEN agent_type = 'WHATSAPP' and Disposition_NEW = 'SENT' THEN 'Not Read'
END AS Disposition_ReadWA,
CASE
 WHEN agent_type = 'WHATSAPP' and Disposition_NEW = 'DELIVERED' THEN 'Not Replied'
 WHEN agent_type = 'WHATSAPP' and Disposition_NEW = 'FAILED' THEN 'Not Replied'
 WHEN agent_type = 'WHATSAPP' and Disposition_NEW = 'READ' THEN 'Not Replied'
 WHEN agent_type = 'WHATSAPP' and Disposition_NEW = 'REPLIED' THEN 'Replied'
 WHEN agent_type = 'WHATSAPP' and Disposition_NEW = 'SENT' THEN 'Not Replied'
END AS Disposition_RepliedWA,
CASE
 WHEN agent_type = 'IVR' and Disposition_NEW = 'ANSWERED' THEN 'Dial IVR'
 WHEN agent_type = 'IVR' and Disposition_NEW = 'NOT_ANSWERED' THEN 'Dial IVR'
 WHEN agent_type = 'IVR' and Disposition_NEW = 'FAILED' THEN 'Dial IVR'
END AS Disposition_DialIVR,
CASE
 WHEN agent_type = 'IVR' and Disposition_NEW = 'ANSWERED' THEN 'Connected'
 WHEN agent_type = 'IVR' and Disposition_NEW = 'NOT_ANSWERED' and contact_number <> '62' THEN 'Connected'
 WHEN agent_type = 'IVR' and Disposition_NEW = 'NOT_ANSWERED' and contact_number = '62' THEN 'Not Connected'
 WHEN agent_type = 'IVR' and Disposition_NEW = 'FAILED' THEN 'Not Connected'
END AS Disposition_ConnectedIVR,
CASE
 WHEN agent_type = 'IVR' and Disposition_NEW = 'ANSWERED' THEN 'Answered'
 WHEN agent_type = 'IVR' and Disposition_NEW = 'NOT_ANSWERED'  THEN 'Not Answered'
 WHEN agent_type = 'IVR' and Disposition_NEW = 'FAILED' THEN 'Not Answered'
END AS Disposition_AnsweredIVR,
CASE
 WHEN agent_type = 'SMS' and Disposition_NEW = 'DELIVERED' THEN 'Triggered SMS'
 WHEN agent_type = 'SMS' and Disposition_NEW = 'SENT'  THEN 'Triggered SMS'
 WHEN agent_type = 'SMS' and Disposition_NEW = 'NOT SENT' THEN 'Triggered SMS'
END AS Disposition_TriggeredSMS,
CASE
 WHEN agent_type = 'SMS' and Disposition_NEW = 'DELIVERED' THEN 'Sent'
 WHEN agent_type = 'SMS' and Disposition_NEW = 'SENT'  THEN 'Sent'
 WHEN agent_type = 'SMS' and Disposition_NEW = 'NOT SENT' THEN 'Not Sent'
END AS Disposition_SentSMS
FROM FUNNEL1),

SRMEDIA AS (
SELECT *,
CASE 
 WHEN disposition = 'FAILED' AND agent_type = 'WHATSAPP' THEN 'FAILED'
 WHEN disposition = 'TRIGGERED' AND agent_type = 'WHATSAPP' THEN 'FAILED'
 WHEN disposition = 'SCHEDULED' AND agent_type = 'WHATSAPP' THEN 'FAILED'
 WHEN disposition = 'REPLIED' AND agent_type = 'WHATSAPP' THEN 'READ'
 WHEN disposition = 'READ' AND agent_type = 'WHATSAPP' THEN 'READ'
 WHEN disposition = 'DELIVERED' AND agent_type = 'WHATSAPP' THEN 'DELIVERED'
 WHEN disposition = 'SENT' AND agent_type = 'WHATSAPP' THEN 'FAILED'
 WHEN disposition = 'DELIVERED' AND agent_type = 'SMS' THEN 'SENT'
 WHEN disposition = 'SENT' AND agent_type = 'SMS' THEN 'SENT'
 WHEN disposition = 'REJECTED' AND agent_type = 'SMS' THEN 'FAILED'
 WHEN disposition = 'UNDELIV' AND agent_type = 'SMS' THEN 'FAILED'
 WHEN disposition = 'EXPIRED' AND agent_type = 'SMS' THEN 'FAILED'
 WHEN disposition = 'UNKNOWN' AND agent_type = 'SMS' THEN 'FAILED'
 WHEN disposition = 'BUFFERED' AND agent_type = 'SMS' THEN 'FAILED'
 WHEN disposition = 'UNDELIVRD' AND agent_type = 'SMS' THEN 'FAILED'
 WHEN disposition = 'REJECTD' AND agent_type = 'SMS' THEN 'FAILED'
 WHEN disposition = 'UNDELIVERD' AND agent_type = 'SMS' THEN 'FAILED'
 WHEN disposition = 'FAILED' AND agent_type = 'SMS' THEN 'FAILED'
 WHEN disposition = 'ANSWERED' AND agent_type = 'IVR' THEN 'ANSWERED'
 WHEN disposition = 'NOT_ANSWERED' AND agent_type = 'IVR' THEN 'NOT_ANSWERED'
 WHEN disposition = 'FAILED' AND agent_type = 'IVR' THEN 'FAILED'
END AS disposition2
FROM FUNNEL1
),

SRWA AS (
SELECT *,
CASE
 WHEN agent_type = 'WHATSAPP' AND disposition = 'REPLIED' THEN 'A'
 WHEN agent_type = 'WHATSAPP' AND disposition = 'READ' THEN 'B'
 WHEN agent_type = 'WHATSAPP' AND disposition = 'DELIVERED' THEN 'C'
 WHEN agent_type = 'WHATSAPP' AND disposition = 'SENT' THEN 'D'
 WHEN agent_type = 'WHATSAPP' AND disposition = 'SCHEDULED' THEN 'E'
 WHEN agent_type = 'WHATSAPP' AND disposition = 'TRIGGERED' THEN 'F'
 WHEN agent_type = 'WHATSAPP' AND disposition = 'FAILED' THEN 'G'
END AS dispositionwa
FROM SRMEDIA
WHERE agent_type = 'WHATSAPP'
),

SRWAPARTISI AS (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY agreementno, angsuran ORDER BY dispositionwa asc) as rn_wa
FROM SRWA
),

SRIVR AS (
SELECT *,
CASE
 WHEN agent_type = 'IVR' AND disposition = 'ANSWERED' THEN 'A'
 WHEN agent_type = 'IVR' AND disposition = 'NOT_ANSWERED' THEN 'B'
 WHEN agent_type = 'IVR' AND disposition = 'FAILED' THEN 'C'
END AS dispositionivr
FROM SRMEDIA
WHERE agent_type = 'IVR'
),

SRIVRPARTISI AS (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY agreementno, angsuran ORDER BY dispositionivr asc) as rn_ivr
FROM SRIVR
),

SRSMS AS (
SELECT *,
CASE 
 WHEN agent_type = 'SMS' AND disposition = 'DELIVERED' THEN 'A'
 WHEN agent_type = 'SMS' AND disposition = 'SENT' THEN 'B'
 WHEN agent_type = 'SMS' AND disposition = 'REJECTED' THEN 'C'
 WHEN agent_type = 'SMS' AND disposition = 'UNDELIV' THEN 'D'
 WHEN agent_type = 'SMS' AND disposition = 'EXPIRED' THEN 'E'
 WHEN agent_type = 'SMS' AND disposition = 'UNKNOWN' THEN 'F'
 WHEN agent_type = 'SMS' AND disposition = 'BUFFERED' THEN 'G'
 WHEN agent_type = 'SMS' AND disposition = 'UNDELIVRD' THEN 'H'
 WHEN agent_type = 'SMS' AND disposition = 'REJECTD' THEN 'I'
 WHEN agent_type = 'SMS' AND disposition = 'UNDELIVERD' THEN 'J'
 WHEN agent_type = 'SMS' AND disposition = 'FAILED' THEN 'K'
END AS dispositionsms
FROM SRMEDIA
WHERE agent_type = 'SMS'
),

SRSMSPARTISI AS (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY agreementno, angsuran ORDER BY dispositionsms asc) as rn_sms
FROM SRSMS
),

BASE_SRMEDIA AS (
SELECT *
FROM SRWAPARTISI
WHERE rn_wa = 1
UNION ALL
SELECT *
FROM SRIVRPARTISI
WHERE rn_ivr = 1
UNION ALL
SELECT *
FROM SRSMSPARTISI
WHERE rn_sms = 1
)

SELECT 
 product,
 angsuran,
 disposition2 as disposition,
 agent_type,
 Provinsi,
 Desa,
 Kab,
 Kec,
 count(agreementno) as agreementno,
 SUM(newSR) as newSR
FROM BASE_SRMEDIA
GROUP BY
 product,
 angsuran,
 disposition2,
 agent_type,
 Provinsi,
 Desa,
 Kab,
 Kec