#################################################################
#最新版RFM
#该模型主要为客户提供会员价值度分析
#该模型可以有三种type，分别为All,Top,Card.
#All的意思是以全体会员作为分析目标
#Top的意思是以时间窗口内会员累计消费金额前N名作为分析目标，N可以更加需求自己定义
#Card的意思是按会员卡级作为分析目标
#################################################################
library(data.table)
library(dplyr)
library(Tmisc)
library(plyr)
library(stringr)
Args = commandArgs()
#####消费数据路径
xiaofei_inpath=Args[6]
xiaofei_inpath_c<-c(xiaofei_inpath)
#####阈值划分表路径,阈值表,分隔
yuzhi_path_1=Args[7]
yuzhi_path<-c(yuzhi_path_1)
#####时间标签
time_1=Args[8]
time_c=c(time_1)
#####输出表1
output1=Args[9]
output1_c<-c(output1)
####输出表2
output2=Args[10]
output2_c<-c(output2)
####输出路径
output3=Args[11]
output3_c<-c(output3)
########################################################################
#读取消费数据并格式转化
all_xiaofei<-function(xiaofei_inpath_c){
  print("开始读取消费数据,即全员消费数据")
  df1<-fread(xiaofei_inpath_c,sep = "\t",stringsAsFactors = F,encoding = "UTF-8",header = T)
  df1<-df1%>%data.frame()
  for(i in 6:7){
    df1[,i]<-plyr::mapvalues(df1[,i],
                             c("\\N","NULL","N/A","null",""),
                             c(0,0,0,0,0))
  }
  df1<-df1%>%mutate_at(vars(src_order_dt),as.Date)
  df1<-df1%>%mutate_at(vars(adj_amt),as.numeric)
  df1<-df1%>%mutate_at(vars(refund_amt),as.numeric)
  #print(head(df1))
  print("消费数据读取完毕")
  return(df1)
}
########################################################################
#RFM_model
RFM_model<-function(RFM_xiaofei,yuzhi_path,time_c,output1_c,output2_c,type_name){
  ######从阈值中提取Top_N，Card_xx等字段
  print("开始阈值解析")
  yuzhi<-fread(yuzhi_path,sep = ",",encoding = "UTF-8",header = F,stringsAsFactors = F)
  yuzhi<-yuzhi%>%data.frame()
  names(yuzhi)<-c("source_type","parameter_type","parameter_start","parameter_end","parameter_name","parameter_class")
  yuzhi_type_name<-names(table(yuzhi$source_type))
  top<-yuzhi_type_name[str_detect(yuzhi_type_name,"Top")]
  top_num<-str_sub(top,5)
  card<-yuzhi_type_name[str_detect(yuzhi_type_name,"Card")]
  print(card)
  # card_name<-lapply(1:length(card),function(i){
  #   str_sub(card[i],6)
  # })
  # card_name_1<-unlist(card_name)
  # print(card_name_1)
  ######计算全员RFM
  if(type_name=="All"){
    print(paste("开始计算",type_name,"的R",sep=""))
    #####提取All的阈值
    sub_yuzhi<-subset(yuzhi,source_type==type_name)
    index_R<-subset(sub_yuzhi,parameter_type=="R")%>%select(c(3,4,5,6))
    index_F<-subset(sub_yuzhi,parameter_type=="F")%>%select(c(3,4,5,6))
    index_M<-subset(sub_yuzhi,parameter_type=="M")%>%select(c(3,4,5,6))
    #####计算R
    R_data<-RFM_xiaofei%>%select(mem_num,src_order_dt)
    R_data<-R_data%>%group_by(mem_num)%>%mutate(days_num=as.integer(as.Date(time_c)-src_order_dt))
    R_data_1<-R_data%>%group_by(mem_num)%>%summarise(R_days=min(days_num))
    R_data_1$R_median<-median(R_data_1$R_days)%>%as.integer()
    R_data_1<-mutate(R_data_1,R_diff=(R_days-R_median))
    print(head(R_data_1))
    
    R_trend<-sapply(1:dim(R_data_1)[1],function(i){
      ifelse(R_data_1$R_diff[i]<0,1,0)
    })
    R_data_2<-cbind(R_data_1,R_trend)
    R_1<-lapply(1:(dim(index_R)[1]),function(i){
      subset(R_data_2,R_days>index_R[i,1]&R_days<=index_R[i,2])
    })
    for(i in 1:(dim(index_R)[1])){
      if(dim(R_1[[i]])>0){
        R_1[[i]]$R_class<-index_R[i,4]
        R_1[[i]]$R_show<-index_R[i,3]
      }
      else {
        print(paste(i,"行R分类有错误,建议调整区间",sep = ""))
      }
    }
    R_1<-rbindlist(R_1)
    print(head(R_1))
    #####计算F
    print(paste("开始计算",type_name,"的F",sep=""))
    F_data<-RFM_xiaofei%>%select(mem_num,src_order_dt)%>%unique()%>%group_by(mem_num)%>%summarise(F_days=n())
    F_data$F_median<-median(F_data$F_days)
    F_data<-mutate(F_data,F_diff=(F_days-F_median))
    F_trend<-sapply(1:dim(F_data)[1],function(i){
      ifelse(F_data$F_diff[i]>0,1,0)
    })
    F_data_1<-cbind(F_data,F_trend)
    F_1<-lapply(1:(dim(index_F)[1]),function(i){
      subset(F_data_1,F_days>index_F[i,1]&F_days<=index_F[i,2])
    })
    for(i in 1:(dim(index_F)[1])){
      if(dim(F_1[[i]])>0){
        F_1[[i]]$F_class<-index_F[i,4]
        F_1[[i]]$F_show<-index_F[i,3]
      }
      else {
        print(paste(i,"行F分类有错误,建议调整区间",sep = ""))
      }
    }
    F_1<-rbindlist(F_1)
    print(head(F_1))
    #####计算M
    print(paste("开始计算",type_name,"的M",sep=""))
    M_data<-RFM_xiaofei%>%select(mem_num,adj_amt,refund_amt)
    M_data_1<-M_data%>%group_by(mem_num)%>%summarise(M_amount=sum(adj_amt)-sum(refund_amt))
    M_data_1$M_median<-median(M_data_1$M_amount)
    M_data_1<-mutate(M_data_1,M_diff=M_amount-M_median)
    M_trend<-sapply(1:dim(M_data_1)[1],function(i){
      ifelse(M_data_1$M_diff[i]>0,1,0)
    })
    M_data_1<-cbind(M_data_1,M_trend)
    M_1<-lapply(1:(dim(index_M)[1]),function(i){
      subset(M_data_1,M_amount>index_M[i,1]&M_amount<=index_M[i,2])
    })
    for(i in 1:(dim(index_M)[1])){
      if(dim(M_1[[i]])>0){
        M_1[[i]]$M_class<-index_M[i,4]
        M_1[[i]]$M_show<-index_M[i,3]
      }
      else {
        print(paste(i,"行M分类有错误,建议调整区间",sep = ""))
      }
    }
    M_1<-rbindlist(M_1)
    print(head(M_1))
    print("开始R、F、M合并")
    temp<-left_join(R_1,F_1,by=c("mem_num"))
    temp<-left_join(temp,M_1,by=c("mem_num"))
    RFM_8<-subset(temp,R_trend==1&F_trend==1&M_trend==1)
    RFM_8$median_class<-8
    RFM_8$median_class_show<-c("重要高价值客户")
    RFM_7<-subset(temp,R_trend==1&F_trend==0&M_trend==1)
    RFM_7$median_class<-7
    RFM_7$median_class_show<-c("重要发展客户")
    RFM_6<-subset(temp,R_trend==0&F_trend==1&M_trend==1)
    RFM_6$median_class<-6
    RFM_6$median_class_show<-c("重要保持客户")
    RFM_5<-subset(temp,R_trend==0&F_trend==0&M_trend==1)
    RFM_5$median_class<-5
    RFM_5$median_class_show<-c("重要挽留客户")
    RFM_4<-subset(temp,R_trend==1&F_trend==1&M_trend==0)
    RFM_4$median_class<-4
    RFM_4$median_class_show<-c("一般价值客户")
    RFM_3<-subset(temp,R_trend==1&F_trend==0&M_trend==0)
    RFM_3$median_class<-3
    RFM_3$median_class_show<-c("一般发展客户")
    RFM_2<-subset(temp,R_trend==0&F_trend==1&M_trend==0)
    RFM_2$median_class<-2
    RFM_2$median_class_show<-c("一般保持客户")
    RFM_1<-subset(temp,R_trend==0&F_trend==0&M_trend==0)
    RFM_1$median_class<-8
    RFM_1$median_class_show<-c("一般挽留客户")
    #####生成表1即h1
    h<-list(RFM_8,RFM_7,RFM_6,RFM_5,RFM_4,RFM_3,RFM_2,RFM_1)
    h1<-rbindlist(h)
    k_means_data<-h1%>%select(R_class,F_class,M_class)
    k_means_class<-kmeans(k_means_data,8)
    h1$julei_class<-k_means_class$cluster
    h1<-h1%>%select(mem_num,R_days,R_show,R_class,F_days,F_show,F_class,M_amount,M_show,M_class,R_trend,F_trend,M_trend,median_class,median_class_show,julei_class)
    names(h1)<-c("会员编号","R实际值","R所属区间","R_class","F实际值","F所属区间","F_class","M实际值","M所属区间","M_class","R_trend","F_trend","M_trend","RFM_class_分类","RFM_class_名称","RFM聚类")
    h1$标签日期<-as.Date(time_c)
    h1$卡类别<-type_name
    print(paste("开始写",type_name,"RFM列表",sep = ""))
    shuchu1<-paste(type_name,output1_c,sep = "")
    shuchu1_1<-paste(output3_c,shuchu1,sep = "")
    write.table(h1,file = shuchu1_1,sep = ",",row.names=FALSE,col.names= FALSE,quote=FALSE,fileEncoding = "UTF-8")
    ######生成表2即h6
    print(paste("开始计算",type_name,"RFM八类需要展示的表",sep=""))
    h2<-h1%>%group_by(RFM_class_名称)%>%summarise(R_min=min(R实际值),R_max=max(R实际值),R_median=median(R实际值)
                                                ,R_avg=mean(R实际值),F_min=min(F实际值),F_max=max(F实际值)
                                                ,F_median=median(F实际值),F_avg=mean(F实际值),M_min=min(M实际值)
                                                ,M_max=max(M实际值),M_median=median(M实际值),M_avg=mean(M实际值)
                                                ,num_person=n()
                                                ,amount_consum=sum(M实际值))%>%mutate(ratio_num_person=num_person/dim(h1)[1],
                                                                                   ratio_amount_consum=amount_consum/sum(amount_consum))
    
    h2$类别<-type_name
    h2$标签日期<-as.Date(time_c)
    shuchu2<-paste(type_name,output2_c,sep = "")
    shuchu2_1<-paste(output3_c,shuchu2,sep = "")
    write.table(h2,file = shuchu2_1,sep = ",",row.names = FALSE,col.names= FALSE,quote = FALSE,fileEncoding = "UTF-8")
    print(paste0("会员人数有",dim(h1)[1]))
    print(paste(type_name,"的RFM计算完毕",sep = ""))
  }
  if(type_name=="Top"){
    #####筛选top消费数据
    print("开始按总消费金额筛选消费数据")
    df<-RFM_xiaofei
    top_xiaofei<-df%>%group_by(mem_num)%>%summarise(adj_amt_top=sum(adj_amt),refund_amt_top=sum(refund_amt))
    top_xiaofei_1<-top_xiaofei%>%mutate(sum_top=(adj_amt_top-refund_amt_top))%>%arrange(desc(sum_top))
    print(dim(top_xiaofei_1))
    print(tail(top_xiaofei_1))
    
    top_int<-as.integer(top_num)
    print(paste("取top",top_int,sep = ""))
    top_xiaofei_2<-top_xiaofei_1[1:top_int,]
    top_xiaofei_3<-left_join(top_xiaofei_2,RFM_xiaofei,by=c("mem_num"))
    top_xiaofei_3<-top_xiaofei_3%>%data.frame()
    print(dim(top_xiaofei_3))
    print(table(top_xiaofei_3$card_type))
    print("按总消费金额筛选完毕")
    #####提取top的阈值
    sub_yuzhi<-subset(yuzhi,source_type==top)
    print(head(sub_yuzhi))
    index_R<-subset(sub_yuzhi,parameter_type=="R")%>%select(c(3,4,5,6))
    index_F<-subset(sub_yuzhi,parameter_type=="F")%>%select(c(3,4,5,6))
    index_M<-subset(sub_yuzhi,parameter_type=="M")%>%select(c(3,4,5,6))
    #####计算R
    print(paste("开始计算",type_name,"的R",sep=""))
    R_data<-top_xiaofei_3%>%select(mem_num,src_order_dt)
    R_data<-R_data%>%group_by(mem_num)%>%mutate(days_num=as.integer(as.Date(time_c)-src_order_dt))
    R_data_1<-R_data%>%group_by(mem_num)%>%summarise(R_days=min(days_num))
    R_data_1$R_median<-median(R_data_1$R_days)%>%as.integer()
    R_data_1<-mutate(R_data_1,R_diff=(R_days-R_median))
    R_trend<-sapply(1:dim(R_data_1)[1],function(i){
      ifelse(R_data_1$R_diff[i]<0,1,0)
    })
    R_data_2<-cbind(R_data_1,R_trend)
    R_1<-lapply(1:(dim(index_R)[1]),function(i){
      subset(R_data_2,R_days>index_R[i,1]&R_days<=index_R[i,2])
    })
    for(i in 1:(dim(index_R)[1])){
      if(dim(R_1[[i]])>0){
        R_1[[i]]$R_class<-index_R[i,4]
        R_1[[i]]$R_show<-index_R[i,3]
      }
      else {
        print(paste(i,"行R分类有错误,建议调整区间",sep = ""))
      }
    }
    R_1<-rbindlist(R_1)
    print(head(R_1))
    #####计算F
    print(paste("开始计算",type_name,"的F",sep=""))
    F_data<-top_xiaofei_3%>%select(mem_num,src_order_dt)%>%unique()%>%group_by(mem_num)%>%summarise(F_days=n())
    F_data$F_median<-median(F_data$F_days)
    F_data<-mutate(F_data,F_diff=(F_days-F_median))
    F_trend<-sapply(1:dim(F_data)[1],function(i){
      ifelse(F_data$F_diff[i]>0,1,0)
    })
    F_data_1<-cbind(F_data,F_trend)
    F_1<-lapply(1:(dim(index_F)[1]),function(i){
      subset(F_data_1,F_days>index_F[i,1]&F_days<=index_F[i,2])
    })
    for(i in 1:(dim(index_F)[1])){
      if(dim(F_1[[i]])>0){
        F_1[[i]]$F_class<-index_F[i,4]
        F_1[[i]]$F_show<-index_F[i,3]
      }
      else {
        print(paste(i,"行F分类有错误,建议调整区间",sep = ""))
      }
    }
    F_1<-rbindlist(F_1)
    print(head(F_1))
    #####计算M
    print(paste("开始计算",type_name,"的M",sep=""))
    M_data<-top_xiaofei_3%>%select(mem_num,adj_amt,refund_amt)
    M_data_1<-M_data%>%group_by(mem_num)%>%summarise(M_amount=sum(adj_amt)-sum(refund_amt))
    M_data_1$M_median<-median(M_data_1$M_amount)
    M_data_1<-mutate(M_data_1,M_diff=M_amount-M_median)
    M_trend<-sapply(1:dim(M_data_1)[1],function(i){
      ifelse(M_data_1$M_diff[i]>0,1,0)
    })
    M_data_1<-cbind(M_data_1,M_trend)
    M_1<-lapply(1:(dim(index_M)[1]),function(i){
      subset(M_data_1,M_amount>index_M[i,1]&M_amount<=index_M[i,2])
    })
    for(i in 1:(dim(index_M)[1])){
      if(dim(M_1[[i]])>0){
        M_1[[i]]$M_class<-index_M[i,4]
        M_1[[i]]$M_show<-index_M[i,3]
      }
      else {
        print(paste(i,"行M分类有错误,建议调整区间",sep = ""))
      }
    }
    M_1<-rbindlist(M_1)
    print(head(M_1))
    print("开始R、F、M合并")
    temp<-left_join(R_1,F_1,by=c("mem_num"))
    temp<-left_join(temp,M_1,by=c("mem_num"))
    RFM_8<-subset(temp,R_trend==1&F_trend==1&M_trend==1)
    RFM_8$median_class<-8
    RFM_8$median_class_show<-c("重要高价值客户")
    RFM_7<-subset(temp,R_trend==1&F_trend==0&M_trend==1)
    RFM_7$median_class<-7
    RFM_7$median_class_show<-c("重要发展客户")
    RFM_6<-subset(temp,R_trend==0&F_trend==1&M_trend==1)
    RFM_6$median_class<-6
    RFM_6$median_class_show<-c("重要保持客户")
    RFM_5<-subset(temp,R_trend==0&F_trend==0&M_trend==1)
    RFM_5$median_class<-5
    RFM_5$median_class_show<-c("重要挽留客户")
    RFM_4<-subset(temp,R_trend==1&F_trend==1&M_trend==0)
    RFM_4$median_class<-4
    RFM_4$median_class_show<-c("一般价值客户")
    RFM_3<-subset(temp,R_trend==1&F_trend==0&M_trend==0)
    RFM_3$median_class<-3
    RFM_3$median_class_show<-c("一般发展客户")
    RFM_2<-subset(temp,R_trend==0&F_trend==1&M_trend==0)
    RFM_2$median_class<-2
    RFM_2$median_class_show<-c("一般保持客户")
    RFM_1<-subset(temp,R_trend==0&F_trend==0&M_trend==0)
    RFM_1$median_class<-8
    RFM_1$median_class_show<-c("一般挽留客户")
    #####生成表1即h1
    h<-list(RFM_8,RFM_7,RFM_6,RFM_5,RFM_4,RFM_3,RFM_2,RFM_1)
    h1<-rbindlist(h)
    k_means_data<-h1%>%select(R_class,F_class,M_class)
    k_means_class<-kmeans(k_means_data,8)
    h1$julei_class<-k_means_class$cluster
    h1<-h1%>%select(mem_num,R_days,R_show,R_class,F_days,F_show,F_class,M_amount,M_show,M_class,R_trend,F_trend,M_trend,median_class,median_class_show,julei_class)
    names(h1)<-c("会员编号","R实际值","R所属区间","R_class","F实际值","F所属区间","F_class","M实际值","M所属区间","M_class","R_trend","F_trend","M_trend","RFM_class_分类","RFM_class_名称","RFM聚类")
    h1$标签日期<-as.Date(time_c)
    h1$卡类别<-type_name
    print(paste("开始写",type_name,"RFM列表",sep = ""))
    shuchu1<-paste(type_name,output1_c,sep = "")
    shuchu1_1<-paste(output3_c,shuchu1,sep = "")
    write.table(h1,file = shuchu1_1,sep = ",",row.names=FALSE,col.names= FALSE,quote=FALSE,fileEncoding = "UTF-8")
    ######生成表2即h6
    print(paste("开始计算",type_name,"RFM八类需要展示的表",sep=""))
    h2<-h1%>%group_by(RFM_class_名称)%>%summarise(R_min=min(R实际值),R_max=max(R实际值),R_median=median(R实际值)
                                                ,R_avg=mean(R实际值),F_min=min(F实际值),F_max=max(F实际值)
                                                ,F_median=median(F实际值),F_avg=mean(F实际值),M_min=min(M实际值)
                                                ,M_max=max(M实际值),M_median=median(M实际值),M_avg=mean(M实际值)
                                                ,num_person=n()
                                                ,amount_consum=sum(M实际值))%>%mutate(ratio_num_person=num_person/dim(h1)[1],
                                                                                   ratio_amount_consum=amount_consum/sum(amount_consum))
    
    h2$类别<-type_name
    h2$标签日期<-as.Date(time_c)
    shuchu2<-paste(type_name,output2_c,sep = "")
    shuchu2_1<-paste(output3_c,shuchu2,sep = "")
    write.table(h2,file = shuchu2_1,sep = ",",row.names = FALSE,col.names= FALSE,quote = FALSE,fileEncoding = "UTF-8")
    print(paste0("会员人数有",dim(h1)[1]))
    print(paste(type_name,"的RFM计算完毕",sep = ""))
  }
  if(type_name=="Card"){
    #####筛选card的消费数据,card_name_1包含了卡名，card_name_c包含了阈值卡名，二者一一对应
    for(i in 1:length(card)){
      #####按卡类别提取消费数据
      RFM_xiaofei_card_type_1<-card[i]
      card_name<-str_replace_all(card[i],"Card_","")
      card_name_1<-str_replace_all(card_name,"\\|",",")
      card_name_2<-unlist(str_split(card_name_1,","))
      print(card_name_2)
      RFM_xiaofei_card<-RFM_xiaofei%>%filter(card_type%in%card_name_2)
      # print(RFM_xiaofei_card_type_1)
      # RFM_xiaofei_card<-subset(RFM_xiaofei,card_type==RFM_xiaofei_card_type_1)
      print(head(RFM_xiaofei_card))
      #####将卡类别对应的阈值提取出来
      yuzhi_card_type_1<-card[i]
      sub_yuzhi<-subset(yuzhi,source_type==yuzhi_card_type_1)
      print(sub_yuzhi)
      index_R<-subset(sub_yuzhi,parameter_type=="R")%>%select(c(3,4,5,6))
      index_F<-subset(sub_yuzhi,parameter_type=="F")%>%select(c(3,4,5,6))
      index_M<-subset(sub_yuzhi,parameter_type=="M")%>%select(c(3,4,5,6))
      #####根据消费数据RFM_xiaofei_card和阈值数据计算RFM
      print(paste("开始计算",RFM_xiaofei_card_type_1,"的R",sep=""))
      R_data<-RFM_xiaofei_card%>%select(mem_num,src_order_dt)
      R_data<-R_data%>%group_by(mem_num)%>%mutate(days_num=as.integer(as.Date(time_c)-src_order_dt))
      R_data_1<-R_data%>%group_by(mem_num)%>%summarise(R_days=min(days_num))
      R_data_1$R_median<-median(R_data_1$R_days)%>%as.integer()
      R_data_1<-mutate(R_data_1,R_diff=(R_days-R_median))
      R_trend<-sapply(1:dim(R_data_1)[1],function(i){
        ifelse(R_data_1$R_diff[i]<0,1,0)
      })
      R_data_2<-cbind(R_data_1,R_trend)
      R_1<-lapply(1:(dim(index_R)[1]),function(i){
        subset(R_data_2,R_days>index_R[i,1]&R_days<=index_R[i,2])
      })
      for(i in 1:(dim(index_R)[1])){
        if(dim(R_1[[i]])>0){
          R_1[[i]]$R_class<-index_R[i,4]
          R_1[[i]]$R_show<-index_R[i,3]
        }
        else {
          print(paste(i,"行R分类有错误,建议调整区间",sep = ""))
        }
      }
      R_1<-rbindlist(R_1)
      print(head(R_1))
      ########F
      print(paste("开始计算",RFM_xiaofei_card_type_1,"的F",sep=""))
      F_data<-RFM_xiaofei_card%>%select(mem_num,src_order_dt)%>%unique()%>%group_by(mem_num)%>%summarise(F_days=n())
      F_data$F_median<-median(F_data$F_days)
      F_data<-mutate(F_data,F_diff=(F_days-F_median))
      F_trend<-sapply(1:dim(F_data)[1],function(i){
        ifelse(F_data$F_diff[i]>0,1,0)
      })
      F_data_1<-cbind(F_data,F_trend)
      F_1<-lapply(1:(dim(index_F)[1]),function(i){
        subset(F_data_1,F_days>index_F[i,1]&F_days<=index_F[i,2])
      })
      for(i in 1:(dim(index_F)[1])){
        if(dim(F_1[[i]])>0){
          F_1[[i]]$F_class<-index_F[i,4]
          F_1[[i]]$F_show<-index_F[i,3]
        }
        else {
          print(paste(i,"行F分类有错误,建议调整区间",sep = ""))
        }
      }
      F_1<-rbindlist(F_1)
      print(head(F_1))
      ########M
      print(paste("开始计算",RFM_xiaofei_card_type_1,"的M",sep=""))
      M_data<-RFM_xiaofei_card%>%select(mem_num,adj_amt,refund_amt)
      M_data_1<-M_data%>%group_by(mem_num)%>%summarise(M_amount=sum(adj_amt)-sum(refund_amt))
      M_data_1$M_median<-median(M_data_1$M_amount)
      M_data_1<-mutate(M_data_1,M_diff=M_amount-M_median)
      M_trend<-sapply(1:dim(M_data_1)[1],function(i){
        ifelse(M_data_1$M_diff[i]>0,1,0)
      })
      M_data_1<-cbind(M_data_1,M_trend)
      M_1<-lapply(1:(dim(index_M)[1]),function(i){
        subset(M_data_1,M_amount>index_M[i,1]&M_amount<=index_M[i,2])
      })
      for(i in 1:(dim(index_M)[1])){
        if(dim(M_1[[i]])>0){
          M_1[[i]]$M_class<-index_M[i,4]
          M_1[[i]]$M_show<-index_M[i,3]
        }
        else {
          print(paste(i,"行M分类有错误,建议调整区间",sep = ""))
        }
      }
      M_1<-rbindlist(M_1)
      print(head(M_1))
      print("开始R、F、M合并")
      temp<-left_join(R_1,F_1,by=c("mem_num"))
      temp<-left_join(temp,M_1,by=c("mem_num"))
      RFM_8<-subset(temp,R_trend==1&F_trend==1&M_trend==1)
      RFM_8$median_class<-8
      RFM_8$median_class_show<-c("重要高价值客户")
      RFM_7<-subset(temp,R_trend==1&F_trend==0&M_trend==1)
      RFM_7$median_class<-7
      RFM_7$median_class_show<-c("重要发展客户")
      RFM_6<-subset(temp,R_trend==0&F_trend==1&M_trend==1)
      RFM_6$median_class<-6
      RFM_6$median_class_show<-c("重要保持客户")
      RFM_5<-subset(temp,R_trend==0&F_trend==0&M_trend==1)
      RFM_5$median_class<-5
      RFM_5$median_class_show<-c("重要挽留客户")
      RFM_4<-subset(temp,R_trend==1&F_trend==1&M_trend==0)
      RFM_4$median_class<-4
      RFM_4$median_class_show<-c("一般价值客户")
      RFM_3<-subset(temp,R_trend==1&F_trend==0&M_trend==0)
      RFM_3$median_class<-3
      RFM_3$median_class_show<-c("一般发展客户")
      RFM_2<-subset(temp,R_trend==0&F_trend==1&M_trend==0)
      RFM_2$median_class<-2
      RFM_2$median_class_show<-c("一般保持客户")
      RFM_1<-subset(temp,R_trend==0&F_trend==0&M_trend==0)
      RFM_1$median_class<-8
      RFM_1$median_class_show<-c("一般挽留客户")
      #####生成表1即h1
      h<-list(RFM_8,RFM_7,RFM_6,RFM_5,RFM_4,RFM_3,RFM_2,RFM_1)
      h1<-rbindlist(h)
      #####表1中添加聚类结果，作为辅助分析
      k_means_data<-h1%>%select(R_class,F_class,M_class)
      k_means_class<-kmeans(k_means_data,8)
      h1$julei_class<-k_means_class$cluster
      h1<-h1%>%select(mem_num,R_days,R_show,R_class,F_days,F_show,F_class,M_amount,M_show,M_class,R_trend,F_trend,M_trend,median_class,median_class_show,julei_class)
      names(h1)<-c("会员编号","R实际值","R所属区间","R_class","F实际值","F所属区间","F_class","M实际值","M所属区间","M_class","R_trend","F_trend","M_trend","RFM_class_分类","RFM_class_名称","RFM聚类")
      h1$标签日期<-as.Date(time_c)
      h1$卡类别<-RFM_xiaofei_card_type_1
      print(paste("开始写",RFM_xiaofei_card_type_1,"的RFM表",sep=""))
      #####将输出的文件名打标记
      shuchu1<-paste(type_name,output1_c,sep = "")
      shuchu1_1<-paste(output3_c,shuchu1,sep = "")
      write.table(h1,file = shuchu1_1,sep = ",",append = TRUE,row.names=FALSE,col.names= FALSE,quote=FALSE,fileEncoding = "UTF-8")
      #####输出表2即h6
      print(paste("开始计算",RFM_xiaofei_card_type_1,"RFM八类需要展示的表",sep=""))
      h2<-h1%>%group_by(RFM_class_名称)%>%summarise(R_min=min(R实际值),R_max=max(R实际值),R_median=median(R实际值)
                                                  ,R_avg=mean(R实际值),F_min=min(F实际值),F_max=max(F实际值)
                                                  ,F_median=median(F实际值),F_avg=mean(F实际值),M_min=min(M实际值)
                                                  ,M_max=max(M实际值),M_median=median(M实际值),M_avg=mean(M实际值)
                                                  ,num_person=n()
                                                  ,amount_consum=sum(M实际值))%>%mutate(ratio_num_person=num_person/dim(h1)[1],
                                                                                     ratio_amount_consum=amount_consum/sum(amount_consum))
      
      
      h2$卡类别<-RFM_xiaofei_card_type_1
      h2$标签日期<-as.Date(time_c)
      shuchu2<-paste(type_name,output2_c,sep = "")
      shuchu2_1<-paste(output3_c,shuchu2,sep = "")
      write.table(h2,file = shuchu2_1,sep = ",",append = TRUE,row.names = FALSE,col.names= FALSE,quote = FALSE,fileEncoding = "UTF-8")
      print(paste(RFM_xiaofei_card_type_1,"的RFM计算完毕",sep = ""))
    }
    
  }
}
func1<-all_xiaofei(xiaofei_inpath_c)
RFM_model(func1,yuzhi_path,time_c,output1_c,output2_c,"All")
print("All执行完毕")
RFM_model(func1,yuzhi_path,time_c,output1_c,output2_c,"Top")
print("Top执行完毕")
RFM_model(func1,yuzhi_path,time_c,output1_c,output2_c,"Card")
print("Card执行完毕")

