package main

import (
	"fmt"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats.go"
	"time"
)

type configuration struct {
	HTTPAddr string `envconfig:"HTTP_ADDR" default:"localhost:8080"`
	Debug    bool   `envconfig:"DEBUG" default:"false"`
	MetricPort string `envconfig:"METRIC_PORT" default:"9000"`

	CephS3Endpoint            string `envconfig:"CEPH_S3_ENDPOINT" default:"api.ceph-go-sar-clustering.svc.k8s.dataline/s3"`
	CephS3Region              string `envconfig:"CEPH_S3_REGION" default:"us-east-1"`
	CephS3DisableSSL          bool   `envconfig:"CEPH_S3_DISABLE_SSL" default:"true"`
	CephS3MaxIdleConns        int    `envconfig:"CEPH_S3_MAX_IDLE_CONNS" default:"512"`
	CephS3MaxIdleConnsPerHost int    `envconfig:"CEPH_S3_MAX_IDLE_CONNS_PER_HOST" default:"512"`
	Bucket                    string `envconfig:"BUCKET" default:"ab-test"`//"com-goods"`
	RecommendationKeyS3       string `envconfig:"RECOMMENDATION_KEY_S3" default:"sup_recommendations_full.csv"`
	NmsKeyS3                  string `envconfig:"NMS_KEY_S3" default:"sup_nms.npy"`

	URLWbxStaticFile    string        `envconfig:"URL_WBX_STATIC_FILE" default:"wbx-content-master.3d.wb.ru:8765/api/v1/file"`
	TimeoutClientStatic time.Duration `envconfig:"TIMEOUT_CLIENT_STATIC" default:"10s"`

	EtcdHosts    []string `envconfig:"ETCD_HOSTS" default:"search-recommend-etcd-1.3d.wb.ru:2379,search-recommend-etcd-1.dp.wb.ru:2379,search-recommend-etcd-1.dl.wb.ru:2379"`
	MutexEtcdKey string   `envconfig:"MUTEX_ETCD_KEY" default:"supplier-recommended-goods"`

	PgDbHost string `envconfig:"PG_DB_HOST" default:"rec-goods.dpl.wb.ru"`
	PgDbPort string `envconfig:"PG_DB_PORT" default:"5432"`
	PgDbPassword string `envconfig:"PG_DB_PASSWORD" default:"vWsxaWN5MhzRxLFiV9LXf"`
	PgDbName string `envconfig:"PG_DB_NAME" default:"rcgds"`
	PgDbUser string `envconfig:"PG_DB_USER" default:"master"`
	PgDbSslMode string `envconfig:"PG_DB_SSL_MODE" default:"disable"`

	ABTestHistoryKeyS3 string `envconfig:"AB_TEST_HISTORY_KEY_S3" default:"ab-test.json"`

}

func main(){
	nc, _ := nats.Connect(nats.DefaultURL, 	nats.UseOldRequestStyle())
	mgr, _ := jsm.New(nc, jsm.WithTimeout(10*time.Second))
	js, _ := nc.JetStream()
	_, _ = mgr.LoadOrNewStream("ORDERS", jsm.Subjects("ORDERS.*"), jsm.MaxAge(24*365*time.Hour), jsm.FileStorage())
	//js.Publish("ORDERS.accept", []byte("hi"), nats.MsgId("ff"))

	_, _ = js.Subscribe("ORDERS.accept", func(msg *nats.Msg) {
		meta, err := msg.Metadata()
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println("Consumer: [ "+meta.Consumer+" ]", "Sequence: [", meta.Sequence.Stream, "]", "Domain: [ "+meta.Domain+" ]",
			"NumDelivered: [", meta.NumDelivered, "]", "NumPending: [", meta.NumPending, "]", "Stream: [", meta.Stream, "]")
		fmt.Println(msg.Header.Get("Nats-Msg-Id"), string(msg.Data))
		//err = js.DeleteMsg("ORDERS",29, nats.Domain(""))
		//if err != nil {
		//	fmt.Println(err)
		//}
	})
	fmt.Println("mes;", nc.InMsgs)

	msg, err := js.GetMsg("ORDERS", 1)
	if err != nil {
		fmt.Println("1", err)
	}
	fmt.Println("!", msg)
	inf, _ := js.StreamInfo("ORDERS")
	fmt.Println(inf.State.Msgs, inf.State.FirstSeq, inf.State.LastSeq)
	sub, err := js.PullSubscribe("ORDERS.accept", "MONITOR")

	msgs, err := sub.Fetch(int(inf.State.Msgs))
	if err != nil {
		fmt.Println("2", err)
	}

	for _, el := range msgs{
		fmt.Println(string(el.Data))

	}


}

//func main() {
//	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stdout))
//	logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)
//	_ = level.Info(logger).Log("msg", "initializing", "version", "dev")
//
//	var cfg configuration
//	if err := envconfig.Process("", &cfg); err != nil {
//		_ = level.Error(logger).Log("msg", "failed to load configuration", "err", err)
//	}
//
//	//aws s3 session
//	awsSession, err := session.NewSession(
//		aws.NewConfig().
//			WithCredentials(credentials.AnonymousCredentials).
//			WithEndpoint(cfg.CephS3Endpoint).
//			WithRegion(cfg.CephS3Region).
//			WithDisableSSL(cfg.CephS3DisableSSL).
//			WithS3ForcePathStyle(true).
//			WithHTTPClient(&http.Client{
//				Transport: &http.Transport{
//					MaxIdleConns:        cfg.CephS3MaxIdleConns,
//					MaxIdleConnsPerHost: cfg.CephS3MaxIdleConnsPerHost,
//				}}),
//	)
//
//	//  create new client ceph go
//	ctx, f := context.WithCancel(context.Background())
//	defer f()
//	cephS3Client := s3Client.NewClient(awsSession)
//	database := cephgo.NewDatabase(cephS3Client, cfg.Bucket, logger)
//
//	err = database.DownloadFile(ctx, "./"+cfg.ABTestHistoryKeyS3, cfg.ABTestHistoryKeyS3)
//	if err != nil {
//		fmt.Println(err)
//	}
//
//	//fd, err := os.Create(cfg.ABTestHistoryKeyS3)
//	data, err := os.ReadFile(cfg.ABTestHistoryKeyS3)
//	if err != nil{
//		fmt.Println(err)
//	}
//	//fd.Close()
//	fmt.Println(string(data))
//	os.Remove(cfg.ABTestHistoryKeyS3)
//
//	//database.UploadFile(ctx, cfg.ABTestHistoryKeyS3, cfg.ABTestHistoryKeyS3)
//}