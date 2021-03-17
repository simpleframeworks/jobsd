# LogC

A common and universal logging interface that every package in the `simple frameworks` org uses. 

[![test status](https://github.com/simpleframeworks/logc/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/simpleframeworks/logc/actions)

Adapters to common logging libraries are provided so whatever logging library that you decide to use can be supported.

```go

type Logger interface {
	Trace(args ...interface{})
	Debug(args ...interface{})
	Info(args ...interface{})
	Warn(args ...interface{})
	Error(args ...interface{})
	WithField(key string, value interface{}) Logger
	WithFields(fields map[string]interface{}) Logger
	WithError(err error) Logger
}

```

### Download it

```
go get -u github.com/simpleframeworks/logc
```

### Logrus example

An outgoing adapter is used to send log messages. Here we use [Logrus](https://github.com/sirupsen/logrus).

```go

log = logc.NewLogrus(logrus.New())

log.Trace("some Trace log")
log.Debug("some Debug log")
log.Info("some Info log")
log.Warn("some Warn log")
log.Error("some Error log")


log.WithField("RequestID",1234).Trace("some Trace log")


log.WithFields(map[string]interface{}{
  "RequestID":  1234,
  "Name":       "SomeName",
}).Info("some Info log")


someError := errors.New("an error occurred")
log.WithError(someError).Error("some Error log")

```

### Gorm adapter

An incoming adapter is used to collect log messages. Here we are collecting logs from [Gorm](https://github.com/go-gorm/gorm)

```go

log = logc.NewLogrus(logrus.New())

log.Info("connecting to db - started")

db, err0 := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{
	Logger: logging.NewGormLogger(log),
})

log.Info("connecting to db - completed")

log.Info("running db auto migration - started")

db.AutoMigrate(&User{})

log.Info("running db auto migration - completed")

user := User{Name: "shmc", Age: 18, Birthday: time.Now()}
result := db.Create(&user)

```

---

Currently only logrus is supported but more packages are planned.