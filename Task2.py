import os
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark import SparkContext
import re
import json

sc = SparkContext()
spark = SparkSession \
        .builder \
        .appName("project-part2") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

# Create the directory to store the result of task2
# if not os.path.exists('Results_Task2_JSON'):
#     os.makedirs('Results_Task2_JSON')
if not os.path.exists('XueXia_Task2_JSON'):
    os.makedirs('XueXia_Task2_JSON')
# Get the list of file name with its column name
cluster1 = ['kevu-8hby.STR_NAME.txt.gz', 'hy4q-igkk.Cross_Street_2.txt.gz', '735p-zed8.EMPCITY.txt.gz', '6kcb-9g8d.independentwebsite.txt.gz', '4qii-5cz9.BOROUGH.txt.gz', 'en2c-j6tw.BRONX_CONDOMINIUM_PROPERTY_Building_Classification.txt.gz', 'cyfw-hfqk.STATEN_ISLAND_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz', 'gpwd-npar.INCIDENT_ADDRESS.txt.gz', 'pvqr-7yc4.Vehicle_Color.txt.gz', 'tg3t-nh4h.BusinessName.txt.gz', '2bmr-jdsv.DBA.txt.gz', 'ajgi-hpq9.BORO.txt.gz', 'dm9a-ab7w.AUTH_REP_LAST_NAME.txt.gz', 'pf3n-zn2m.StreetAddress.txt.gz', 'jz4z-kudi.Violation_Location__City_.txt.gz', 'eamj-ryxu.Agency.txt.gz', '2sps-j9st.PERSON_FIRST_NAME.txt.gz', 'faiq-9dfq.Vehicle_Color.txt.gz', 'c284-tqph.Vehicle_Make.txt.gz', '2bnn-yakx.Vehicle_Body_Type.txt.gz', 'dj4e-3xrn.SCHOOL_LEVEL_.txt.gz', 'ac4n-c5re.PRINCIPAL_PHONE_NUMBER.txt.gz', 'i8ys-e4pm.CORE_COURSE_9_12_ONLY_.txt.gz', 'qgea-i56i.PREM_TYP_DESC.txt.gz', 'vrn4-2abs.SCHOOL_LEVEL_.txt.gz', 'n8p9-7jxp.EMPCITY.txt.gz', '7btz-mnc8.Provider_Last_Name.txt.gz', 'fbaw-uq4e.Location_1.txt.gz', 'kiv2-tbus.Vehicle_Color.txt.gz', 'bty7-2jhb.Site_Safety_Mgr_s_Last_Name.txt.gz', 'pvqr-7yc4.Vehicle_Body_Type.txt.gz', '72ss-25qh.Agency_ID.txt.gz', 'dm9a-ab7w.STREET_NAME.txt.gz', 'ub9e-s7ai.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz', 'uq7m-95z8.interest3.txt.gz', 'erm2-nwe9.Landmark.txt.gz', 'feu5-w2e2.BusinessZip.txt.gz', 'mdcw-n682.Agency_Acronym.txt.gz', 'cznr-hmrv.School_Name.txt.gz', 'pdpg-nn8i.SCHOOL_NAME.txt.gz', 'u35m-9t32.Address.txt.gz', 'ji82-xba5.address.txt.gz', 'kiv2-tbus.Vehicle_Body_Type.txt.gz', 'nre2-6m2s.Business_City.txt.gz', '2bnn-yakx.Vehicle_Make.txt.gz', 'pchn-eaxn.School_Name.txt.gz', 'pq5i-thsu.DVC_MAKE.txt.gz', 'a6zp-tcs3.Agency.txt.gz', '3rfa-3xsf.Cross_Street_2.txt.gz', '5nz7-hh6t.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz', '3rfa-3xsf.Street_Name.txt.gz', 'p2d7-vcsb.COMPLAINT_INQUIRY_STREET_ADDRESS.txt.gz', 'qu8g-sxqf.First_Name.txt.gz', '956m-xy24.COMPARABLE_RENTAL_1__Building_Classification.txt.gz', 'jhjm-vsp8.Agency.txt.gz', 'k3cd-yu9d.CANDMI.txt.gz', '956m-xy24.MANHATTAN_CONDOMINIUM_PROPERTY_Building_Classification.txt.gz', 'diks-hcwd.School_Name.txt.gz', 'pvqr-7yc4.Vehicle_Make.txt.gz', 'p2d7-vcsb.ACCOUNT_PHONE.txt.gz', '6ypq-ih9a.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz', 'kz72-dump.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz', '4n2j-ut8i.SCHOOL_LEVEL_.txt.gz', 'ytjm-yias.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz', 'xck4-5xd5.phone.txt.gz', 'upwt-zvh3.SCHOOL_LEVEL_.txt.gz', '43nn-pn8j.DBA.txt.gz', '5uac-w243.Lat_Lon.txt.gz', 'xx4a-msrm.Address_1.txt.gz', '3btx-p4av.COMPARABLE_RENTAL___2__Building_Classification.txt.gz', '6je4-4x7e.SCHOOL_LEVEL_.txt.gz', 'uq7m-95z8.website.txt.gz', 'wvts-6tdf.Address.txt.gz', '6wcu-cfa3.CORE_COURSE__MS_CORE_and_9_12_ONLY_.txt.gz', 'uq7m-95z8.interest4.txt.gz', 'faiq-9dfq.Vehicle_Body_Type.txt.gz', '8jfz-tjny.Agency.txt.gz', '72ss-25qh.Borough.txt.gz', 'h9gi-nx95.VEHICLE_TYPE_CODE_2.txt.gz', 'h9gi-nx95.VEHICLE_TYPE_CODE_3.txt.gz', 'ic3t-wcy2.Applicant_s_First_Name.txt.gz', 'aiww-p3af.Incident_Zip.txt.gz', 'jxyc-rxiv.MANHATTAN___COOPERATIVES_COMPARABLE_PROPERTIES___Neighborhood.txt.gz', 'tvhc-4n5n.REGISTRY_ADDRESS.txt.gz', 'pvqr-7yc4.Vehicle_Make.txt.gz', '2bnn-yakx.Vehicle_Make.txt.gz', 'cvh6-nmyi.SCHOOL_LEVEL_.txt.gz', 'kwmq-dbub.CANDMI.txt.gz', 'yahh-6yjc.School_Type.txt.gz', '99br-frp6.BOROUGH___COMMUNITY.txt.gz', 's3k6-pzi2.interest2.txt.gz', 'ad4c-mphb.MANHATTAN_CONDOMINIUM_PROPERTY_Building_Classification.txt.gz', 'ajxm-kzmj.NeighborhoodName.txt.gz', 'gez6-674h.CORE_SUBJECT___MS_CORE_and__9_12_ONLY_.txt.gz', '2bnn-yakx.Vehicle_Color.txt.gz', '4twk-9yq2.CrossStreet2.txt.gz', 'mjux-q9d4.SCHOOL_LEVEL_.txt.gz', 'ptev-4hud.Zip.txt.gz', 'mqdy-gu73.Color.txt.gz', 'uq7m-95z8.interest2.txt.gz', 'qcdj-rwhu.BUSINESS_NAME2.txt.gz', 'feu5-w2e2.BusinessCity.txt.gz', 'by6m-6zpb.interest.txt.gz', 'vw9i-7mzq.website.txt.gz', 'upwt-zvh3.SCHOOL.txt.gz', 'c284-tqph.Vehicle_Color.txt.gz', 'gpwd-npar.LOCATION.txt.gz', 'ipu4-2q9a.Site_Safety_Mgr_s_First_Name.txt.gz', 'bty7-2jhb.Owner_s_House_City.txt.gz', 'sqcr-6mww.School_Name.txt.gz', 'vw9i-7mzq.interest6.txt.gz', 'qbjq-atxv.Agency.txt.gz', 'as69-ew8f.StartCity.txt.gz', 'f7qh-bcr5.CORE_SUBJECT___MS_CORE_and__9_12_ONLY_.txt.gz', 'vw9i-7mzq.interest2.txt.gz', '6anw-twe4.FirstName.txt.gz', 'ph7v-u5f3.TOP_VEHICLE_MODELS___5.txt.gz', '2xh6-psuq.Project_School_Name.txt.gz', 'gahm-hu5h.BRONX___COOPERATIVES_COMPARABLE_PROPERTIES___Neighborhood.txt.gz', 'h9gi-nx95.VEHICLE_TYPE_CODE_5.txt.gz', '2v9c-2k7f.DBA.txt.gz', 'aiww-p3af.Park_Facility_Name.txt.gz', 'fxs2-faah.PRINCIPAL_PHONE_NUMBER.txt.gz', 'sxmw-f24h.Park_Facility_Name.txt.gz', 'jz4z-kudi.Violation_Location__Zip_Code_.txt.gz', 'ydkf-mpxb.CrossStreetName.txt.gz', 'ci93-uc8s.Address1.txt.gz', 'n8p9-7jxp.ZIP.txt.gz', '5uac-w243.PREM_TYP_DESC.txt.gz', 'uq7m-95z8.interest5.txt.gz', 'bjuu-44hx.DVV_MAKE.txt.gz', 'jt7v-77mi.Vehicle_Color.txt.gz', '4pt5-3vv4.Location.txt.gz', '52dp-yji6.Owner_Last_Name.txt.gz', 'jz4z-kudi.Respondent_Address__City_.txt.gz', 'h9gi-nx95.VEHICLE_TYPE_CODE_1.txt.gz', 'tu4y-7fre.Boro.txt.gz', 'k3cd-yu9d.BOROUGH_CITY.txt.gz', 'a5qt-5jpu.STATEN_ISLAND_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz', 'ajgi-hpq9.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz', 'p937-wjvj.HOUSE_NUMBER.txt.gz', 'xne4-4v8f.SCHOOL_LEVEL_.txt.gz', 'fgq8-am2v.Website.txt.gz', 's3k6-pzi2.neighborhood.txt.gz', 'fzv4-jan3.SCHOOL_LEVEL_.txt.gz', '956m-xy24.MANHATTAN_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz', 'as69-ew8f.TruckMake.txt.gz', '735p-zed8.CANDMI.txt.gz', 'jz4z-kudi.Respondent_Address__Zip_Code_.txt.gz', '2sps-j9st.PERSON_LAST_NAME.txt.gz', 'yggg-xf4b.Website.txt.gz', 'cwqt-nvfg.Boro.txt.gz', 'cgz5-877h.SCHOOL_LEVEL_.txt.gz', '7btz-mnc8.Provider_First_Name.txt.gz', '8k4x-9mp5.Last_Name__only_2014_15_.txt.gz', 'cspg-yi7g.ADDRESS.txt.gz', 'pgtq-ht5f.CORE_SUBJECT___MS_CORE_and__9_12_ONLY_.txt.gz', 'tqtj-sjs8.FromStreetName.txt.gz', 'en2c-j6tw.BRONX_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz', 'w9ak-ipjd.Applicant_Last_Name.txt.gz', '3aka-ggej.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz', 'fp78-wt5b.address.txt.gz', 'a9md-ynri.MI.txt.gz', 'ci93-uc8s.Website.txt.gz', 'jt7v-77mi.Vehicle_Make.txt.gz', 'pvqr-7yc4.Vehicle_Body_Type.txt.gz', '3rfa-3xsf.Incident_Zip.txt.gz', 'xk6g-r83g.Agency.txt.gz', 's79c-jgrm.EMPCITY.txt.gz', 'vx8i-nprf.MI.txt.gz', 'erm2-nwe9.Park_Facility_Name.txt.gz', 'faiq-9dfq.Vehicle_Body_Type.txt.gz', 'hy4q-igkk.School_Name.txt.gz', 'itd7-gx3g.Location_1.txt.gz', 'bdjm-n7q4.CrossStreet2.txt.gz', 'bs8b-p36w.STREET.txt.gz', 'gahm-hu5h.COMPARABLE_RENTAL___2__Neighborhood.txt.gz', '52dp-yji6.Owner_First_Name.txt.gz', '3h2n-5cm9.STREET.txt.gz', 'p937-wjvj.STREET_NAME.txt.gz', 'y4fw-iqfr.Location_1.txt.gz', '9z9b-6hvk.Borough.txt.gz', 'uwyv-629c.StreetName.txt.gz', 'ci93-uc8s.Vendor_DBA.txt.gz', 'i6b5-j7bu.TOSTREETNAME.txt.gz', '6kcb-9g8d.neighborhood.txt.gz', 'a5td-mswe.Vehicle_Color.txt.gz', 'uzcy-9puk.Street_Name.txt.gz', 'rbx6-tga4.Applicant_Business_Address.txt.gz', '8isn-pgv3.Owner_Telephone.txt.gz', 'qbce-2kcu.COMPARABLE_RENTAL___2__Neighborhood.txt.gz', 'fp78-wt5b.phonenumber.txt.gz', '6je4-4x7e.SCHOOL.txt.gz', '7jkp-5w5g.Agency.txt.gz', 'b9km-gdpy.ZIP.txt.gz', 'kyad-zm4j.Location.txt.gz', 'kiv2-tbus.Vehicle_Make.txt.gz', 'sxmw-f24h.Location.txt.gz', '4fnu-iufz.Agency.txt.gz', 'kiyv-ks3f.Website.txt.gz', 'mrxb-9w9v.BOROUGH___COMMUNITY.txt.gz', 'hy4q-igkk.Street_Name.txt.gz', '3rfa-3xsf.School_Phone_Number.txt.gz', 'urzf-q2g5.Phone_Number.txt.gz', 'ffnc-f3aa.SCHOOL_LEVEL_.txt.gz', 'c284-tqph.Vehicle_Make.txt.gz', 'e9xc-u3ds.CANDMI.txt.gz', 'vhah-kvpj.Borough.txt.gz', 'dm9a-ab7w.OWNER_ZIP.txt.gz', 'bbs3-q5us.EMPCITY.txt.gz', '8gr8-ngjc.phone.txt.gz', '62mr-ukqs.BROOKLYN___COOPERATIVES_COMPARABLE_PROPERTIES___Building_Classification.txt.gz', 'x5tk-fa54.Agency_Name.txt.gz', '72mk-a8z7.ORGANIZATION_PHONE.txt.gz', 'aiww-p3af.Intersection_Street_2.txt.gz', '8i43-kna8.CORE_SUBJECT.txt.gz', '6rrm-vxj9.parkname.txt.gz', '3aka-ggej.BOROUGH.txt.gz', 'dm9a-ab7w.APPLICANT_FIRST_NAME.txt.gz', 'sybh-s59s.CORE_SUBJECT___MS_CORE_and__9_12_ONLY_.txt.gz', 'yayv-apxh.SCHOOL_LEVEL_.txt.gz', 'bdjm-n7q4.Location.txt.gz', 'kj4p-ruqc.StreetName.txt.gz', 'vr8p-8shw.DVT_MAKE.txt.gz', 'wks3-66bn.School_Name.txt.gz', 't8hj-ruu2.Business_Phone_Number.txt.gz', 'ipu4-2q9a.Site_Safety_Mgr_s_Last_Name.txt.gz', '7yds-6i8e.CORE_SUBJECT__MS_CORE_and_9_12_ONLY_.txt.gz', '72ss-25qh.Website.txt.gz', 's3k6-pzi2.interest3.txt.gz', 'tmr6-dfvn.School_Name.txt.gz', 't8hj-ruu2.First_Name.txt.gz', 'jxyc-rxiv.COMPARABLE_RENTAL___2__Building_Classification.txt.gz', 'bawj-6bgn.BRONX_CONDOMINIUM_PROPERTY_Building_Classification.txt.gz', 'mdcw-n682.First_Name.txt.gz', 'ykx2-pdw8.QUEENS___COOPERATIVES_COMPARABLE_PROPERTIES___Building_Classification.txt.gz', 'w9ak-ipjd.Filing_Representative_City.txt.gz', 'vw9i-7mzq.interest5.txt.gz', 'weg5-33pj.SCHOOL_LEVEL_.txt.gz', '8vgb-zm6e.City__State__Zip_.txt.gz', 'sv2w-rv3k.BORO.txt.gz', 'sqcr-6mww.Street_Name.txt.gz', 'cwg5-cqkm.QUEENS___COOPERATIVES_COMPARABLE_PROPERTIES___Building_Classification.txt.gz', 'n84m-kx4j.VEHICLE_MAKE.txt.gz', '3rfa-3xsf.Location.txt.gz', 'uzcy-9puk.Park_Facility_Name.txt.gz', 'qjvp-rnsx.Website.txt.gz', 'aiww-p3af.Cross_Street_2.txt.gz', 'mdcw-n682.Last_Name.txt.gz', 'w9ak-ipjd.Filing_Representative_First_Name.txt.gz', 'w9ak-ipjd.Owner_s_Business_Name.txt.gz', 'pqg4-dm6b.Address1.txt.gz', 'dm9a-ab7w.AUTH_REP_FIRST_NAME.txt.gz', 'bss9-579f.BROOKLYN_____CONDOMINIUMS_COMPARABLE_PROPERTIES_____Neighborhood.txt.gz', 'sxx4-xhzg.Park_Site_Name.txt.gz', 'myei-c3fa.Neighborhood_3.txt.gz', 'k4xi-fxp5.Borough.txt.gz', 'h9gi-nx95.VEHICLE_TYPE_CODE_4.txt.gz', 'd3ge-anaz.CORE_COURSE__MS_CORE_and_9_12_ONLY_.txt.gz', '5nz7-hh6t.BORO.txt.gz', 'qu8g-sxqf.MI.txt.gz', '2bnn-yakx.Vehicle_Body_Type.txt.gz', 'kiv2-tbus.Vehicle_Make.txt.gz', 'mdcw-n682.Middle_Initial.txt.gz', 'nhms-9u6g.Name__Last__First_.txt.gz', 'm3fi-rt3k.Agency_.txt.gz', '3rfa-3xsf.School_Name.txt.gz', 'sybh-s59s.SCHOOL_NAME.txt.gz', '6anw-twe4.LastName.txt.gz', '9b9u-8989.DBA.txt.gz', 'dpm2-m9mq.applicant_zip.txt.gz', 'qpm9-j523.org_neighborhood.txt.gz']
# cluster1 = ['ykx2-pdw8.QUEENS___COOPERATIVES_COMPARABLE_PROPERTIES___Building_Classification.txt.gz', 'w9ak-ipjd.Filing_Representative_City.txt.gz', 'vw9i-7mzq.interest5.txt.gz', 'weg5-33pj.SCHOOL_LEVEL_.txt.gz', '8vgb-zm6e.City__State__Zip_.txt.gz', 'sv2w-rv3k.BORO.txt.gz', 'sqcr-6mww.Street_Name.txt.gz', 'cwg5-cqkm.QUEENS___COOPERATIVES_COMPARABLE_PROPERTIES___Building_Classification.txt.gz', 'n84m-kx4j.VEHICLE_MAKE.txt.gz', '3rfa-3xsf.Location.txt.gz', 'uzcy-9puk.Park_Facility_Name.txt.gz', 'qjvp-rnsx.Website.txt.gz', 'aiww-p3af.Cross_Street_2.txt.gz', 'mdcw-n682.Last_Name.txt.gz', 'w9ak-ipjd.Filing_Representative_First_Name.txt.gz', 'w9ak-ipjd.Owner_s_Business_Name.txt.gz', 'pqg4-dm6b.Address1.txt.gz', 'dm9a-ab7w.AUTH_REP_FIRST_NAME.txt.gz', 'bss9-579f.BROOKLYN_____CONDOMINIUMS_COMPARABLE_PROPERTIES_____Neighborhood.txt.gz', 'sxx4-xhzg.Park_Site_Name.txt.gz', 'myei-c3fa.Neighborhood_3.txt.gz', 'k4xi-fxp5.Borough.txt.gz', 'h9gi-nx95.VEHICLE_TYPE_CODE_4.txt.gz', 'd3ge-anaz.CORE_COURSE__MS_CORE_and_9_12_ONLY_.txt.gz', '5nz7-hh6t.BORO.txt.gz', 'qu8g-sxqf.MI.txt.gz', '2bnn-yakx.Vehicle_Body_Type.txt.gz', 'kiv2-tbus.Vehicle_Make.txt.gz', 'mdcw-n682.Middle_Initial.txt.gz', 'nhms-9u6g.Name__Last__First_.txt.gz', 'm3fi-rt3k.Agency_.txt.gz', '3rfa-3xsf.School_Name.txt.gz', 'sybh-s59s.SCHOOL_NAME.txt.gz', '6anw-twe4.LastName.txt.gz', '9b9u-8989.DBA.txt.gz', 'dpm2-m9mq.applicant_zip.txt.gz', 'qpm9-j523.org_neighborhood.txt.gz']
# cluster3 = ['5694-9szk.Business_Website_or_Other_URL.txt.gz', 'uwyv-629c.StreetName.txt.gz', 'faiq-9dfq.Vehicle_Color.txt.gz', 'qcdj-rwhu.BUSINESS_NAME2.txt.gz', '6ypq-ih9a.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz', 'pvqr-7yc4.Vehicle_Color.txt.gz', 'en2c-j6tw.BRONX_CONDOMINIUM_PROPERTY_Building_Classification.txt.gz', 'uq7m-95z8.interest6.txt.gz', '5ziv-wcy4.WEBSITE.txt.gz', 'ydkf-mpxb.CrossStreetName.txt.gz', 'w9ak-ipjd.Applicant_Last_Name.txt.gz', 'jz4z-kudi.Respondent_Address__City_.txt.gz', 'rbx6-tga4.Owner_Street_Address.txt.gz', 'sqmu-2ixd.Agency_Name.txt.gz', 'aiww-p3af.Incident_Zip.txt.gz', 'mmvm-mvi3.Org_Name.txt.gz', 'h9gi-nx95.VEHICLE_TYPE_CODE_5.txt.gz', 'uh2w-zjsn.Borough.txt.gz', 'tqtj-sjs8.FromStreetName.txt.gz', 'mqdy-gu73.Color.txt.gz', '7jkp-5w5g.Agency.txt.gz', 's3zn-tf7c.QUEENS_CONDOMINIUM_PROPERTY_Building_Classification.txt.gz', 'sqcr-6mww.School_Name.txt.gz', 'vrn4-2abs.SCHOOL_LEVEL_.txt.gz', '2sps-j9st.PERSON_LAST_NAME.txt.gz', '2bmr-jdsv.DBA.txt.gz', '4d7f-74pe.Address.txt.gz', 'ji82-xba5.address.txt.gz', 'hy4q-igkk.School_Name.txt.gz', 's9d3-x4fz.EMPCITY.txt.gz', '5uac-w243.PREM_TYP_DESC.txt.gz', '64gx-bycn.EMPCITY.txt.gz', 'e9xc-u3ds.CANDMI.txt.gz', 'h9gi-nx95.VEHICLE_TYPE_CODE_3.txt.gz', 'p937-wjvj.HOUSE_NUMBER.txt.gz', 'dj4e-3xrn.SCHOOL_LEVEL_.txt.gz', 'qu8g-sxqf.MI.txt.gz', 'mdcw-n682.Middle_Initial.txt.gz', 'pq5i-thsu.DVC_MAKE.txt.gz', 'ub9e-s7ai.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz', '52dp-yji6.Owner_First_Name.txt.gz', 'jz4z-kudi.Respondent_Address__Zip_Code_.txt.gz', 'vx8i-nprf.MI.txt.gz', 'k3cd-yu9d.Location_1.txt.gz', 'p6h4-mpyy.PRINCIPAL_PHONE_NUMBER.txt.gz', 'sybh-s59s.CORE_SUBJECT___MS_CORE_and__9_12_ONLY_.txt.gz', 'kz72-dump.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz', '7btz-mnc8.Provider_Last_Name.txt.gz', 'ph7v-u5f3.TOP_VEHICLE_MODELS___5.txt.gz', 'mjux-q9d4.SCHOOL_LEVEL_.txt.gz', 'hjvj-jfc9.Borough.txt.gz', 'h9gi-nx95.VEHICLE_TYPE_CODE_2.txt.gz', 'easq-ubfe.CITY.txt.gz', 'sv2w-rv3k.BORO.txt.gz', 'qu8g-sxqf.First_Name.txt.gz', 'ipu4-2q9a.Site_Safety_Mgr_s_First_Name.txt.gz', 'ipu4-2q9a.Site_Safety_Mgr_s_Last_Name.txt.gz', 'pgtq-ht5f.CORE_SUBJECT___MS_CORE_and__9_12_ONLY_.txt.gz', '52dp-yji6.Owner_Last_Name.txt.gz', 's3k6-pzi2.interest4.txt.gz', '4y63-yw9e.SCHOOL_NAME.txt.gz', 'gez6-674h.CORE_SUBJECT___MS_CORE_and__9_12_ONLY_.txt.gz', 'a9md-ynri.MI.txt.gz', 'u553-m549.Independent_Website.txt.gz', 'uzcy-9puk.Street_Name.txt.gz', 'dg92-zbpx.VendorAddress.txt.gz', 'jcih-dj9q.QUEENS_____CONDOMINIUMS_COMPARABLE_PROPERTIES_____Neighborhood.txt.gz', '735p-zed8.CANDMI.txt.gz', 'vg63-xw6u.CITY.txt.gz', 'aiww-p3af.Cross_Street_1.txt.gz', 'sa5w-dn2t.Agency.txt.gz', 'cspg-yi7g.ADDRESS.txt.gz', 'crbs-vur7.QUEENS_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz', 'erm2-nwe9.City.txt.gz', 'nyis-y4yr.Owner_s__Phone__.txt.gz', 'tukx-dsca.Address_1.txt.gz', '9b9u-8989.DBA.txt.gz', 'e4p3-6ecr.Agency_Name.txt.gz', '5mw2-hzqx.BROOKLYN_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz', 'kiv2-tbus.Vehicle_Make.txt.gz', 'w6yt-hctp.COMPARABLE_RENTAL_1__Building_Classification.txt.gz', 'k3cd-yu9d.CANDMI.txt.gz', 'ii2w-6fne.Borough.txt.gz', 'w7w3-xahh.Location.txt.gz', 'erm2-nwe9.Park_Facility_Name.txt.gz', '5nz7-hh6t.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz', '8wbx-tsch.Website.txt.gz', 'xne4-4v8f.SCHOOL_LEVEL_.txt.gz', 'vw9i-7mzq.neighborhood.txt.gz', 'yayv-apxh.SCHOOL_LEVEL_.txt.gz', 'aiww-p3af.Park_Facility_Name.txt.gz', 'jz4z-kudi.Violation_Location__City_.txt.gz', 'kiv2-tbus.Vehicle_Body_Type.txt.gz', 'fzv4-jan3.SCHOOL_LEVEL_.txt.gz', 'w7w3-xahh.Address_ZIP.txt.gz', 'i9pf-sj7c.INTEREST.txt.gz', 'ci93-uc8s.ZIP.txt.gz', 'jtus-srrj.School_Name.txt.gz', 'a5td-mswe.Vehicle_Color.txt.gz', '29bw-z7pj.Location_1.txt.gz', 'vw9i-7mzq.interest4.txt.gz', 'pvqr-7yc4.Vehicle_Make.txt.gz', '3rfa-3xsf.Incident_Zip.txt.gz', 'faiq-9dfq.Vehicle_Body_Type.txt.gz', 'pvqr-7yc4.Vehicle_Body_Type.txt.gz', 'kj4p-ruqc.StreetName.txt.gz', '4pt5-3vv4.Location.txt.gz', 'c284-tqph.Vehicle_Make.txt.gz', 'pqg4-dm6b.Address1.txt.gz', 'cqc8-am9x.Borough.txt.gz', '6rrm-vxj9.parkname.txt.gz', 'tg4x-b46p.ZipCode_s_.txt.gz', 'jzt2-2f7h.School_Name.txt.gz', 'ci93-uc8s.Website.txt.gz', 'm59i-mqex.QUEENS_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz', 'uzcy-9puk.School_Phone_Number.txt.gz', 'ci93-uc8s.Vendor_DBA.txt.gz', 'cyfw-hfqk.STATEN_ISLAND_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz', '3rfa-3xsf.Intersection_Street_2.txt.gz', 'uqxv-h2se.neighborhood.txt.gz', 'w9ak-ipjd.Owner_s_Street_Name.txt.gz', 'h9gi-nx95.VEHICLE_TYPE_CODE_1.txt.gz', '4twk-9yq2.CrossStreet2.txt.gz', 'fbaw-uq4e.CITY.txt.gz', 'mdcw-n682.First_Name.txt.gz', 'w7w3-xahh.Address_City.txt.gz', 'i4ni-6qin.PRINCIPAL_PHONE_NUMBER.txt.gz', 'imfa-v5pv.School_Name.txt.gz', 'sxx4-xhzg.Park_Site_Name.txt.gz', 'vw9i-7mzq.interest1.txt.gz', 'sqcr-6mww.Cross_Street_1.txt.gz', '6anw-twe4.FirstName.txt.gz', '2bnn-yakx.Vehicle_Body_Type.txt.gz', 'uzcy-9puk.Park_Facility_Name.txt.gz', 'pvqr-7yc4.Vehicle_Make.txt.gz', 'c284-tqph.Vehicle_Color.txt.gz', 'm56g-jpua.COMPARABLE_RENTAL___1___Building_Classification.txt.gz', 'tsak-vtv3.Upcoming_Project_Name.txt.gz', 'tg3t-nh4h.BusinessName.txt.gz', 'cgz5-877h.SCHOOL_LEVEL_.txt.gz', 'jz4z-kudi.Violation_Location__Zip_Code_.txt.gz', 'us4j-b5zt.Agency.txt.gz', 'vr8p-8shw.DVT_MAKE.txt.gz', '3qfc-4tta.BRONX_____CONDOMINIUMS_COMPARABLE_PROPERTIES_____Neighborhood.txt.gz', 'bawj-6bgn.BRONX_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz', 'ci93-uc8s.fax.txt.gz', 'ffnc-f3aa.SCHOOL_LEVEL_.txt.gz', 'h9gi-nx95.VEHICLE_TYPE_CODE_4.txt.gz', 'rbx6-tga4.Owner_Street_Address.txt.gz', 's3k6-pzi2.interest5.txt.gz', '2sps-j9st.PERSON_FIRST_NAME.txt.gz', 'ji82-xba5.street.txt.gz', 'f7qh-bcr5.CORE_SUBJECT___MS_CORE_and__9_12_ONLY_.txt.gz', '3rfa-3xsf.Street_Name.txt.gz', 'n84m-kx4j.VEHICLE_MAKE.txt.gz', 'hy4q-igkk.Location.txt.gz', 'sxmw-f24h.Cross_Street_2.txt.gz', 'yahh-6yjc.School_Type.txt.gz', '72ss-25qh.Agency_ID.txt.gz', 'faiq-9dfq.Vehicle_Body_Type.txt.gz', 'm56g-jpua.MANHATTAN___COOPERATIVES_COMPARABLE_PROPERTIES___Building_Classification.txt.gz', '3rfa-3xsf.School_Name.txt.gz', 'ic3t-wcy2.Applicant_s_First_Name.txt.gz', 'vw9i-7mzq.interest3.txt.gz', 'i6b5-j7bu.TOSTREETNAME.txt.gz', 'i5ef-jxv3.Agency.txt.gz', '7crd-d9xh.website.txt.gz', 'mdcw-n682.Last_Name.txt.gz', 'ge8j-uqbf.interest.txt.gz', 'q2ni-ztsb.Street_Address_1.txt.gz', '8k4x-9mp5.Last_Name__only_2014_15_.txt.gz', 'wks3-66bn.School_Name.txt.gz', '43nn-pn8j.DBA.txt.gz', 'qgea-i56i.PREM_TYP_DESC.txt.gz', 'bdjm-n7q4.CrossStreet2.txt.gz', 'nhms-9u6g.Name__Last__First_.txt.gz', 'bdjm-n7q4.Location.txt.gz', 'x3kb-2vbv.School_Name.txt.gz', 'uzcy-9puk.Location.txt.gz', '6anw-twe4.LastName.txt.gz', 'tyfh-9h2y.BROOKLYN___COOPERATIVES_COMPARABLE_PROPERTIES___Building_Classification.txt.gz', '3rfa-3xsf.Cross_Street_2.txt.gz', 'bty7-2jhb.Site_Safety_Mgr_s_Last_Name.txt.gz', '9jgj-bmct.Incident_Address_Street_Name.txt.gz', 'pdpg-nn8i.BORO.txt.gz', 'w9ak-ipjd.Owner_s_Business_Name.txt.gz', 'rb2h-bgai.Website.txt.gz', 'jt7v-77mi.Vehicle_Make.txt.gz', 'as69-ew8f.TruckMake.txt.gz', 'mrxb-9w9v.BOROUGH___COMMUNITY.txt.gz', 'pvqr-7yc4.Vehicle_Body_Type.txt.gz', 'dm9a-ab7w.AUTH_REP_LAST_NAME.txt.gz', '9z9b-6hvk.Borough.txt.gz', 'wv4q-e75v.STATEN_ISLAND_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz', 'kwmq-dbub.CANDMI.txt.gz', 'dvzp-h4k9.COMPARABLE_RENTAL_____1_____Building_Classification.txt.gz', '6ypq-ih9a.BOROUGH.txt.gz', 'p2d7-vcsb.ACCOUNT_CITY.txt.gz', '2v9c-2k7f.DBA.txt.gz', 'erm2-nwe9.Landmark.txt.gz', 'dm9a-ab7w.APPLICANT_FIRST_NAME.txt.gz', '72ss-25qh.Borough.txt.gz', 'qpm9-j523.org_neighborhood.txt.gz', '6wcu-cfa3.CORE_COURSE__MS_CORE_and_9_12_ONLY_.txt.gz', 'nfkx-wd79.Address_1.txt.gz', 'jzdn-258f.Agency.txt.gz', 'kiv2-tbus.Vehicle_Color.txt.gz', 'w9ak-ipjd.Filing_Representative_First_Name.txt.gz', 'irhv-jqz7.BROOKLYN___COOPERATIVES_COMPARABLE_PROPERTIES___Building_Classification.txt.gz', 'm3fi-rt3k.Street_Address_1_.txt.gz', 'ipu4-2q9a.Owner_s_House_City.txt.gz', 'qpm9-j523.org_website.txt.gz', 'qgea-i56i.Lat_Lon.txt.gz', 'jvce-szsb.Website.txt.gz', 'd3ge-anaz.CORE_COURSE__MS_CORE_and_9_12_ONLY_.txt.gz', 'kiyv-ks3f.phone.txt.gz', 'qe6k-pu9t.Agency.txt.gz', '5e7x-8jy6.School_Name.txt.gz', 'xne4-4v8f.SCHOOL.txt.gz', '7btz-mnc8.Provider_First_Name.txt.gz', 'uq7m-95z8.interest1.txt.gz', 'n5mv-nfpy.Location1.txt.gz', '8i43-kna8.CORE_SUBJECT.txt.gz', 'eccv-9dzr.Telephone_Number.txt.gz', '4n2j-ut8i.SCHOOL_LEVEL_.txt.gz', 'dm9a-ab7w.STREET_NAME.txt.gz', '2bnn-yakx.Vehicle_Make.txt.gz', '2bnn-yakx.Vehicle_Color.txt.gz', '2bnn-yakx.Vehicle_Body_Type.txt.gz', 'jt7v-77mi.Vehicle_Color.txt.gz', 'bty7-2jhb.Owner_s_House_Zip_Code.txt.gz', 'cvh6-nmyi.SCHOOL_LEVEL_.txt.gz', '7yds-6i8e.CORE_SUBJECT__MS_CORE_and_9_12_ONLY_.txt.gz', 'ajxm-kzmj.NeighborhoodName.txt.gz', '3aka-ggej.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz', '6bgk-3dad.RESPONDENT_ZIP.txt.gz', 'fbaw-uq4e.Location_1.txt.gz', 'jxyc-rxiv.MANHATTAN___COOPERATIVES_COMPARABLE_PROPERTIES___Building_Classification.txt.gz', 'n2s5-fumm.BRONX_CONDOMINIUM_PROPERTY_Building_Classification.txt.gz', 'bjuu-44hx.DVV_MAKE.txt.gz', 'uzcy-9puk.Street_Name.txt.gz', 's3k6-pzi2.interest1.txt.gz', 'wg9x-4ke6.Principal_phone_number.txt.gz', 'vhah-kvpj.Borough.txt.gz', 'dm9a-ab7w.AUTH_REP_FIRST_NAME.txt.gz', '3rfa-3xsf.Street_Name.txt.gz', 'urzf-q2g5.Phone_Number.txt.gz', 'him9-7gri.Agency.txt.gz', '3rfa-3xsf.Cross_Street_2.txt.gz', 'mu46-p9is.CallerZipCode.txt.gz', 'a5qt-5jpu.STATEN_ISLAND_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz', 'ytjm-yias.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz', 'sxmw-f24h.Park_Facility_Name.txt.gz', 'vuae-w6cg.Agency.txt.gz', 'qusa-igsv.BORO.txt.gz', '5tdj-xqd5.Borough.txt.gz', '2bnn-yakx.Vehicle_Make.txt.gz', 't8hj-ruu2.Business_Phone_Number.txt.gz', 'ajgi-hpq9.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz', 'jhjm-vsp8.Agency.txt.gz', '4nft-bihw.Property_Address.txt.gz', '6je4-4x7e.SCHOOL_LEVEL_.txt.gz', 'c284-tqph.Vehicle_Make.txt.gz', 'dpm2-m9mq.owner_zip.txt.gz', 'gk83-aa6y.SCHOOL_NAME.txt.gz', 't8hj-ruu2.First_Name.txt.gz', 'as69-ew8f.StartCity.txt.gz', 'i8ys-e4pm.CORE_COURSE_9_12_ONLY_.txt.gz', 'myei-c3fa.Neighborhood_1.txt.gz', 'upwt-zvh3.SCHOOL_LEVEL_.txt.gz', 'aiww-p3af.School_Phone_Number.txt.gz', 'kiv2-tbus.Vehicle_Make.txt.gz', 'weg5-33pj.SCHOOL_LEVEL_.txt.gz', 'rmv8-86p4.BROOKLYN_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz']




# Using dictionary as strategy:
def check_person_name(value):
    lastname = ['smith', 'johnson','williams','jones','brown','davis','miller','wilson','moore','taylor','anderson','thomas',\
                'jackson','white','harris','martin','thompson','garcia','Martinez','robinson','ford']
    firstname =['rakib','james','anthony','jacob','daniel','alexander','michael','william','jayden','ethan','ryan',\
                'emma','isabella','madison','ava','ashley','chloe','olivia','emily','paola',\
                'andrew','christopher', 'daniel', 'frank', 'george', 'jeffrey', 'jennifer', 'john', 'kevin', 'elizabeth', 'michael'
                ]
    lastname.extend(firstname)
    for name in lastname:
        if name in value.lower():
            return True
    return False

def check_business_name(value):
    business_suffix = ['inc','inc.','llc','l.l.c.','p.c.','pc','lp','l.p.','corp','corp.','corporation','company','co.','co','ltd','ltd.','capital','holdings','services']
    for business in business_suffix:
        if business in value.lower():
            return True
    return False

def check_school_level(value):
    school_level = ['K-8','K-2','K-3' 'Elementary', 'Elementary School', 'Middle', 'Middle School', 'Transfer', 'High',
                    'High School','D75','YABC']
    for i in range(len(school_level)):
        school_level[i] = school_level[i].lower()
    if value.lower() in school_level:
        return True
    else:
        return False

def check_color(value):
    color_list = ['white', 'black', 'pink', 'purple', 'blue', 'gray', 'green', 'beige', 'yellow', \
                      'ivory', 'gold', 'orange', 'red', 'brown']
    color_short = ['BK','WH','GR','RD','BL','GY','SL']
    for color in color_list:
        if color in color_short:
            return True
        elif color in value.lower():
            return True
    return False

def check_car_make(value):
    car_make = ['BMW', 'TOYOT', 'AUDI', 'NISSA', 'DODGE', 'ACURA', 'HYUND', 'MINI', 'LEXUS', 'SUBAR', \
                    'HONDA', 'KIA', 'CHEVR', 'FORD', 'BUICK', 'VOLKS', 'GMC', 'VOLVO', 'INFIN', 'JEEP',\
                'ACADE','AERO','AJAX','ALEXA','ALFAR','AMC','ANCIA','APRIL','AS/M','AUSTI','AUTOC','BENTL',\
                'BERTO','BL/B','BL/BI','BRIDG','CADIL','CARGO','CHECK','CHRYS','DORSE','DUCAT','FERRA',\
                'FIAT','FRUEH','HARLE','HIN','HINO','HUMME','INTER','ISUZU','JAGUA','KAWAS','KENWO',\
                'LINCO','MACK','MASSA','MAZDA','MERCU','MINI','MITSU','NS/OT',\
                ]

    for brand in car_make:
        if brand in value.upper():
            return True
    return False

def check_city(value):
    city_list = ['new york', 'albany', 'amsterdam', 'auburn', 'buffalo', 'batavia', 'beacon', 'binghamton',
                     'rochester', 'yonkers', \
                     'syracuse', 'new rochelle', 'cheektowaga', 'mount vernon', 'schenectady', 'brentwood', 'utica',
                     'white plains', \
                     'freeport', 'hempstead', 'levittown', 'troy', 'hicksville']
    for city in city_list:
        if city in value.lower():
            return True
    return False

def check_borough(value):
    abbreviation = ["K","M","Q","R","X"]
    borough_list = ['brooklyn', 'bronx', 'queens', 'manhattan', 'staten island']
    if (len(value) == 1) & (value in abbreviation):
        return True

    for borough in borough_list:
        if borough in value.lower():
            return True
    return False

def check_vehicle_type(value):
    type_list = ["AMBULANCE","VAN","TAXI","BUS","SDN","SUBN","12PU","2C","2CV","2D","2DR",
    '2DSD','4DSD','AMBU','ATV','BOAT','BUS','CMIX','CONV','CUST','DCOM','DELV','DUMP','EMVR',
    'FIRE','FLAT','FPM','H/IN','HRSE','H/TR','H/WH','LIM','LOCO','LSV','LSVT','LTRL','MCC',
    'MCY','MFH','MOPD','PICK','POLE','P/SH','RBM','RD/S','REFG','RPLC','R/RD','SEDN','SEMI',
    'SNOW','SN/P','S/SP','STAK','SUBN','SWT','TANK','T/CR','TOW','TRAC','TRAV','TR/C','TR/E',
    'TRLR','UTIL','VAN','W/DR','W/SR']

    for type in type_list:
        if type in value.lower():
            return True
    return False

def check_neighborhood(value):
    manhattan = ['Alphabet','Battery Park','Carnegie Hill','Chelsea','Chinatown','East Harlem','East Village',\
                'Financial District','Flatiron District','Gramercy Park','Greenwich Village','Harlem','Clinton','Inwood',\
                'Kips Bay','Lincoln Square','Lower East Side','Manhattan Valley','Midtown East','Midtown West',\
                'Morningside Heights','Murray Hill','NoLita','Little Italy','Roosevelt Island','SoHo','Tribeca','Upper East Side',\
                'Upper West Side','Washington Heights','West Village']
    bronx =['Baychester','Bedford Park','Belmont','Bronxdale','Castle Hill','City Island','Concourse Village',\
            'Grand Concourse','Morrisania','Country Club','Fieldston','Fordham','Hunts Point','Kingsbridge','Kingsbridge Heights',\
            'Melrose','Morris Heights','Morris Park','Mott Haven','Parkchester','Pelham Bay','Pelham Gardens','Pelham Parkway',\
            'Port Morris','Riverdale','Soundview','Throgs Neck','Tremont','University Heights','Wakefield','Williamsbridge','Woodlawn']
    brooklyn =['Bath Beach','Bay Ridge','Bedford-Stuyvesant','Bensonhurst','Bergen Beach','Boerum Hill','Borough Park',\
            'Brighton Beach','Brooklyn Heights','Brownsville','Bushwick','Canarsie','Carroll Gardens','Cobble Hill','Coney Island',\
            'Crown Heights','Cypress Hills','Downtown Brooklyn','Dumbo','Vinegar Hill','Dyker Heights','East New York','Flatbush',\
            'Flatlands','Fort Greene','Clinton Hill','Gerritsen Beach','Gowanus','Gravesend','Greenpoint','Greenwood Heights',\
            'Manhattan Beach','Marine Park','Midwood','Mill Basin','Park Slope','Prospect Heights','Prospect Park South','Kensington',\
            'Red Hook','Sea Gate','Sheepshead Bay','Sunset Park','Williamsburg','Windsor Terrace']
    queens = ['Arverne','Astoria','Bayside','Beechhurst','Belle Harbor','Neponsit','Bellerose','Briarwood','Broad Channel','Cambria Heights',\
            'College Point','Corona','Douglaston','East Elmhurst','Elmhurst','Far Rockaway','Floral Park','Flushing','Forest Hills',\
            'Fresh Meadows','Glen Oaks','Glendale','Hillcrest','Hollis','Hollis Hills','Howard Beach','Jackson Heights','Jamaica',\
            'Jamaica Estates','Jamaica Hills','Kew Gardens','Laurelton','Little Neck','Long Island City','Maspeth','Middle Village',\
            'Oakland Gardens','Ozone Park','Queens Village','Rego Park','Richmond Hill','Ridgewood','Rockaway Park','Rosedale',\
            'South Jamaica','South Ozone Park','Springfield Gardens','St. Albans','Sunnyside','Whitestone','Woodhaven','Woodside']
    staten = ['Annadale','Arden Heights','Arrochar','Bay Street','Bulls Head','Castleton Corners','Charleston','Clifton','Dongan Hills',\
            'Eltingville','Emerson Hill','Graniteville','Grant City','Grasmere','Concord','Great Kills','Grymes Hill','Huguenot',\
            'Livingston','Manor Heights','Mariners Harbor','Midland Beach','New Brighton','New Dorp','New Dorp Beach','New Springville',\
            'Oakwood','Pleasant Plains','Port Richmond','Prince Bay','Richmondtown','Rosebank','Rossville','Shore Acres','Silver Lake',\
            'South Beach','St. George','Stapleton','Sunnyside','Todt Hill','Tompkinsville','Tottenville','Travis','West New Brighton',\
            'Westerleigh','Willowbrook','Woodrow']
    all_neighborhoods = []
    all_neighborhoods.extend(manhattan)
    all_neighborhoods.extend(bronx)
    all_neighborhoods.extend(brooklyn)
    all_neighborhoods.extend(queens)
    all_neighborhoods.extend(staten)
    for neighborhood in all_neighborhoods:
        if neighborhood.lower() in value.lower():
            return True
    return False

def check_school_name(value):
    schools = ['school','high school','secondary school','college','academy','university']
    for school in schools:
        if school in value.lower():
            return True
    return False

def check_colleage_university_name(value):
    list = ['college','university','academy']
    for name in list:
        if name in value.lower():
            return True
    return False

def check_address(value):
    street_name = ['street','st.','avenue','boulevard']
    for street in street_name:
        if street in value.lower() and value[0].isdigit() == True:
            return True
    return False

def check_street_name(value):

    street_name = ['street','st.','avenue','boulevard','east','west','north','south','cross']

    for street in street_name:
        if street in value.lower() and value[0].isdigit() == False:
            return True
    return False

def check_city_agency(value):
    agencies = ['311','NYPD','NYCHA','NEW YORK CITY HOUSING AUTHORITY','SCA','SCHOOL CONSTRUCTION AUTHORITY',\
                'SBS','SMALL BUSINESS SERVICES','DCLA','CULTURAL AFFAIRS','DOE','DEPARTMENT OF EDUCATION',\
                'HPD','HOUSING PRESERVATION AND DEVELOPMENT','DYCD','DEPARTMENT OF YOUTH & COMMUNITY DEVELOPMENT',\
                'DOF','DEPARTMENT OF FINANCE','DOB','DEPARTMENT OF BUILDINGS','ACS','ADMIN FOR CHILDREN\'S SERVICES',\
                'NYCEM','DEPARTMENT OF EMERGENCY MANAGEMENT','NYPL','NEW YORK PUBLIC LIBRARY','DCAS',\
                'DEPARTMENT OF CITYWIDE ADMIN SERVICE','CCHR','COMMISSION ON HUMAN RIGHTS','DPR','DEPARTMENT OF PARKS AND RECREATION',\
                'FD','FIRE DEPARTMENT','TLC','TAXI AND LIMOUSINE COMMISSION','DOHMH','DEPARTMENT OF HEALTH AND MENTAL HYGIENE','CUNY',\
                'CITY UNIVERSITY OF NEW YORK','DS','DEPARTMENT OF SANITATION']
    for agency in agencies:
        if agency in value.upper():
            return True
    return False

def check_subjects(value):
    subjects = ['math', 'algebra','geometry','science','biology','physics','chemistry','geography','history',\
                'citizenship','physical education','p.e','art','music','business','economics','social',\
                'english','teaching','film','health']
    for subject in subjects:
        if subject in value.lower():
            return True
    return False

# Using regular expression
def check_zip_code(value):
    regex = re.compile(r"(^[0-9]{5}(?:-[0-9]{4})?$)")
    if re.fullmatch(regex,value) != None:
        return True
    else:
        return False

# print("Test Zip code: ")
# print(check_zip_code("11421"))
# print(check_zip_code("11201-1122"))
# print(check_zip_code("1123432435"))
# print(check_zip_code("1"))
# print(check_zip_code("csd222"))

def check_website(value):
    regex = re.compile(r'(http|ftp|https):\/\/[\w\-_]+(\.[\w\-_]+)+([\w\-\.,@?^=%&amp;:/~\+#]*[\w\-\@?^=%&amp;/~\+#])?')
    if re.fullmatch(regex,value) != None:
        return True
    final = value[-4:-1]
    final_list = ['.ORG','.NET','.COM','.EDU']
    if final in final_list:
        return True
    return False

# print("Test websites: ")
# print(check_website("http://schools.nyc.gov/schoolportals/15/k001"))
# print(check_website("http://4angelsdaycareinc.com"))
# print(check_website("http://7thavenuecenter.org"))
# print(check_website("www.wwwwwwwwwwwww"))
# print(check_website("HTTPS://WWW.WETHINKOUT.COM"))


def check_LAT_LON_coordinates(value):
    try:
        lag = value.split(',')[0][1:-1]
        s = r'^[\-\+]?(0(\.\d{1,10})?|([1-9](\d)?)(\.\d{1,10})?|1[0-7]\d{1}(\.\d{1,10})?|180\.0{1,10})$'  # longitude
        if re.match(s, lag) != None:
            return True
        else:
            return False
    except:
        return False

# Other strategy
def check_phone_number(value):
    if len(value) == 12:
        for i, c in enumerate(value):
            if i in [3, 7]:
                if c != '-':
                    return False
            elif not c.isalnum():
                return False
        return True
    elif len(value) == 10:
        for i in value:
            if i.isdigit() == False:
                return False
        return False
    else:
        return False
#201 461-5200, (718) 706-7755

def check_location_type(value):
    locations = ['building','airport',' bank','church','clothing','boutique','abandoned','shelter','store','supermarket',\
                 'hotel','grocery','residence','hospital','restaurant','transit','subway','parking']
    for location in locations:
        if location in value.lower():
            return True
    return False

def check_parks_playgrounds(value):
    locations = ['park','playground','garden']
    for loc in locations:
        if loc in value.lower():
            return True
    return False

def check_CANDMI(value):
    if len(value) == 1 & value.isupper():
        return True
    return False

# def check_phone_number(value):
#     if re.match(r'^\d{10}$',value) != None:
#         return True
#     elif re.match(r'^\d{3}\-\d{3}\-\d{4}$',value) != None:
#         return True
#     elif re.match(r'^\d{3}\d{3}\-\d{4}$',value) != None:
#         return True
#     elif re.match(r'^\d{3}\-\d{3}\d{4}$',value) != None:
#         return True
#     else:
#         return False

# print("Test Phone number: ")
# print(check_phone_number("234-234-1344"))
# print(check_phone_number("3wf-22-22"))
# print(check_phone_number("234-223-134322"))
# print(check_phone_number("2342134442"))

# https://www1.nyc.gov/assets/finance/jump/hlpbldgcode.html Building Classification | City of New York
def check_building_classify(value):
    first = value[0:1]
    second = value[1:2]
    third = value[2:3]

    if (first.isupper() & second.isdigit()) & (third == '-'):
        return True
    else:
        return False

# print("Test building classify: ")
# print(check_building_classify("C6-WALK-UP"))
# print(check_building_classify("D0-ELEVATOR"))
# print(check_building_classify("r1-ccc"))



# Start deal with data
have_result = 0
no_result = []
for each in cluster1:
    flag = 0
    filepath = '/Users/xiaxue/PycharmProjects/FinalTask2/NYCColumns/' + each
    DF = spark.read.option("header", "true").option("delimiter", "\t").csv(filepath).cache()
    rows = DF.collect()
    column_name = each
    column_name = each.split(".")[1]
    devise = {}
    devise["column_name"] = column_name
    devise["semantic_types"] = {}
    for i in range(len(rows)):
        instance_name = rows[i][0].strip()
        length = len(rows[i])
        if length <= 1:
            continue
        instance_fre = int(rows[i][1])
        if instance_name == None:
            continue
        if (check_zip_code(instance_name) == True):
            label_name = "Zip code"
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = {}
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_person_name(instance_name) == True):
            label_name = "Person name"
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = {}
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_business_name(instance_name) == True):
            label_name = "Businesss name"
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = {}
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_school_level(instance_name) == True):
            label_name = "School Levels"
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = {}
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_color(instance_name) == True):
            label_name = "Color"
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = {}
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_car_make(instance_name) == True):
            label_name = "Car make"
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = {}
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_city(instance_name) == True):
            label_name = "City"
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = {}
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_borough(instance_name) == True):
            label_name = "Borough"
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = {}
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_vehicle_type(instance_name) == True):
            label_name = "Vehicle Type"
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = {}
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_neighborhood(instance_name) == True):
            label_name = "Neighborhood"
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = {}
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_school_name(instance_name) == True):
            label_name = "School name"
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = {}
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_colleage_university_name(instance_name) == True):
            label_name = "College/University names"
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = {}
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_address(instance_name) == True):
            label_name = "Address"
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = {}
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_street_name(instance_name) == True):
            label_name = "Street name"
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = {}
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_city_agency(instance_name) == True):
            label_name = "City agency"
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = {}
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_subjects(instance_name) == True):
            label_name = "Subjects in school"
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = {}
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_website(instance_name) == True):
            label_name = "Websites"
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = {}
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_LAT_LON_coordinates(instance_name) == True):
            label_name = "LAT/LON coordinates"
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = {}
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_phone_number(instance_name) == True):
            label_name = "Phone number"
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = {}
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_location_type(instance_name) == True):
            label_name = "Type of location"
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = {}
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_parks_playgrounds(instance_name) == True):
            label_name = "Parks/Playgrounds"
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = {}
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_building_classify(instance_name) == True):
            label_name = "Building Classification"
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = {}
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if (check_CANDMI(instance_name) == True):
            label_name = "CANDMI"
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = {}
                value_dic[label_name] = instance_fre
        else:
            flag += 1

        if flag == 23:
            label_name = "other"
            value_dic = devise["semantic_types"]
            if label_name in value_dic.keys():
                value_dic[label_name] += instance_fre
            else:
                value_dic[label_name] = {}
                value_dic[label_name] = instance_fre

# Get JSON file as output:
# { "column_name": string
#    "semantic_types": [
#        { "semantic_type": "Business name" (type: string)
#          "count": the number of type(type: integer)
#       },
#    ...
#    ]
    l = []
    for key, value in devise["semantic_types"].items():
        new_dict = {}
        new_dict['semantic_type'] = key
        new_dict['count'] = value
        l.append(new_dict)
    devise['semantic_types'] = l
    each = each.split(".")
    # json_file_path = each[0] + '.' + each[1]
    json_file_path = "task2.json"
    json_file_path='XueXia_Task2_JSON/'+ json_file_path
    with open(json_file_path, 'a', newline='\n') as json_file:
        json.dump(devise, json_file)
        have_result += 1


# # run_result, hand_devise are dictionary in this form:
# # {"sxmw-f24h.Location":"LAT/LON coordinates;Address;Street name",...}
# # key is "sxmw-f24h.Location", value is "LAT/LON coordinates;Address;Street name"}
#

# def get_accuracy(run_result, hand_devise):
#     count_correct = 0
#     count = 0
#     for key in run_result.iterkeys():
#         count += 1
#         run_key = run_result[key].split(";")
#         hand_key = hand_devise[key].split(";")
#         for each in run_key:
#             if each in hand_key:
#                 count_correct += 1
#     accuracy = count_correct / count
#     return accuracy


