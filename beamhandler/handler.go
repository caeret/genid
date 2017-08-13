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

func (s *DefaultHandler) Handle(req beam.Request) (beam.Response, error) {
	var resp beam.Response
	switch strings.ToUpper(string(req[0])) {
	case "PING":
		resp = beam.NewSimpleStringsResponse("PONG")
	case "INCR":
		if len(req) != 2 {
			resp = beam.NewErrorsResponse("invalid arguments")
		} else {
			id, err := s.engine.Next(string(req[1]))
			if err != nil {
				resp = beam.NewErrorsResponse(err.Error())
			} else {
				resp = beam.NewIntegersResponse(int(id))
			}
		}
	case "GET":
		if len(req) != 2 {
			resp = beam.NewErrorsResponse("invalid arguments")
		} else {
			id, err := s.engine.Current(string(req[1]))
			if err != nil {
				resp = beam.NewErrorsResponse(err.Error())
			} else {
				resp = beam.NewSimpleStringsResponse(strconv.FormatInt(id, 10))
			}
		}
	default:
		resp = beam.NewErrorsResponse("unsupported method.")
	}
	return resp, nil
}
