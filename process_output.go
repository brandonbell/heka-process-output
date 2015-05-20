/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2012
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Brandon Bell (remuso@gmail.com)
#
# ***** END LICENSE BLOCK *****/

package process

import (
	"fmt"
	"github.com/mozilla-services/heka/message"
	. "github.com/mozilla-services/heka/pipeline"
	"io"
        "time"
//        "regexp"
)

//var varMatcher *regexp.Regexp

type ProcessOutputConfig struct {
	Command cmdConfig
	TimeoutSeconds uint     `toml:"timeout"`
}

func (po *ProcessOutput) ConfigStruct() interface{} {
	return &ProcessOutputConfig{
		TimeoutSeconds: 5,
	}
}

type ProcessOutput struct {
	conf	*ProcessOutputConfig
}

func (po *ProcessOutput) Init(config interface{}) (err error) {
	po.conf = config.(*ProcessOutputConfig)
	return
}

func (po *ProcessOutput) Run(or OutputRunner, h PluginHelper) (err error) {
	inChan := or.InChan()

	var (
		pack    *PipelinePack
		msg     *message.Message
		payload string
	)

	for pack = range inChan {
		msg = pack.Message
		payload = msg.GetPayload()
                
                err = po.RunCommand(payload, msg.Fields)

		if err != nil {
			or.LogError(err)
		}

		pack.Recycle()
	}
	return
}

func (po *ProcessOutput) RunCommand(payload string, fields []*message.Field) (err error) {
        args := append(po.conf.Command.Args, payload)
//        for idx, arg := range po.conf.Command.Args {
//           if match := varMatcher.FindStringSubmatch(po.conf.Command.Args); match != nil { 
//               po.conf.Command.Args[idx] = 
            
            
            
	c := NewManagedCmd(po.conf.Command.Bin, args,
		time.Duration(po.conf.TimeoutSeconds)*time.Second)

	var cmdin io.WriteCloser
	if cmdin, err = c.StdinPipe(); err != nil {
		return
	}
	defer cmdin.Close()

	if err = c.Start(false); err != nil {
		return
	}

	if _, err = fmt.Fprintf(cmdin, "%s", payload); err != nil {
		return
	}

	cmdin.Close()

	err = c.Wait()
	return
}

func init() {
//        varMatcher, _ = regexp.Compile("\\%(\\w+)%")
	RegisterPlugin("ProcessOutput", func() interface{} {
		return new(ProcessOutput)
	})
}
