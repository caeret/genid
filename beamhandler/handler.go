package beamhandler

import (
	"strconv"
	"strings"

	"github.com/gaemma/beam"
	"github.com/gaemma/genid/generator"
)

type DefaultHandler struct {
	engine generator.Generator
}

func NewHandler(engine generator.Generator) *DefaultHandler {
	s := new(DefaultHandler)
	s.engine = engine
	return s
}

func (s *DefaultHandler) Handle(req *beam.Request) (beam.Reply, error) {
	var resp beam.Reply
	switch strings.ToUpper(req.GetStr(0)) {
	case "PING":
		resp = beam.NewSimpleStringsReply("PONG")
	case "INCR":
		if req.Len() != 2 {
			resp = beam.NewErrorsReply("invalid arguments")
		} else {
			id, err := s.engine.Next(req.GetStr(1))
			if err != nil {
				resp = beam.NewErrorsReply(err.Error())
			} else {
				resp = beam.NewIntegersReply(int(id))
			}
		}
	case "GET":
		if req.Len() != 2 {
			resp = beam.NewErrorsReply("invalid arguments")
		} else {
			id, err := s.engine.Current(req.GetStr(1))
			if err != nil {
				resp = beam.NewErrorsReply(err.Error())
			} else {
				resp = beam.NewSimpleStringsReply(strconv.FormatInt(id, 10))
			}
		}
	default:
		resp = beam.NewErrorsReply("unsupported method.")
	}
	return resp, nil
}
