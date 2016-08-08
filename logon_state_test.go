package quickfix

import (
	"testing"
	"time"

	"github.com/quickfixgo/quickfix/enum"
	"github.com/quickfixgo/quickfix/internal"
	"github.com/stretchr/testify/suite"
)

type LogonStateTestSuite struct {
	SessionSuite
}

func TestLogonStateTestSuite(t *testing.T) {
	suite.Run(t, new(LogonStateTestSuite))
}

func (s *LogonStateTestSuite) SetupTest() {
	s.Init()
	s.session.stateMachine.State = logonState{}
}

func (s *LogonStateTestSuite) TestIsLoggedOn() {
	s.False(s.session.IsLoggedOn())
}

func (s *LogonStateTestSuite) TestTimeoutLogonTimeout() {
	s.Timeout(s.session, internal.LogonTimeout)
	s.State(latentState{})
}

func (s *LogonStateTestSuite) TestTimeoutNotLogonTimeout() {
	tests := []internal.Event{internal.PeerTimeout, internal.NeedHeartbeat, internal.LogoutTimeout}

	for _, test := range tests {
		s.Timeout(s.session, test)
		s.State(logonState{})
	}
}

func (s *LogonStateTestSuite) TestTimeoutSessionExpire() {
	s.session.store.IncrNextTargetMsgSeqNum()
	s.session.store.IncrNextSenderMsgSeqNum()

	s.Timeout(s.session, internal.SessionExpire)
	s.State(latentState{})
	s.NextTargetMsgSeqNum(1)
	s.NextSenderMsgSeqNum(1)
	s.NoMessageSent()
	s.NoMessageQueued()
}

func (s *LogonStateTestSuite) TestFixMsgInNotLogon() {
	s.FixMsgIn(s.session, s.NewOrderSingle())

	s.mockApp.AssertExpectations(s.T())
	s.State(latentState{})
	s.NextTargetMsgSeqNum(1)
}

func (s *LogonStateTestSuite) TestFixMsgInLogon() {
	s.store.IncrNextSenderMsgSeqNum()
	s.messageFactory.seqNum = 1
	s.store.IncrNextTargetMsgSeqNum()

	logon := s.Logon()
	logon.Body.SetField(tagHeartBtInt, FIXInt(32))

	s.mockApp.On("FromAdmin").Return(nil)
	s.mockApp.On("OnLogon")
	s.mockApp.On("ToAdmin")
	s.FixMsgIn(s.session, logon)

	s.mockApp.AssertExpectations(s.T())

	s.State(inSession{})
	s.Equal(32*time.Second, s.session.heartBeatTimeout)

	s.LastToAdminMessageSent()
	s.MessageType(enum.MsgType_LOGON, s.mockApp.lastToAdmin)
	s.FieldEquals(tagHeartBtInt, 32, s.mockApp.lastToAdmin.Body)

	s.NextTargetMsgSeqNum(3)
	s.NextSenderMsgSeqNum(3)
}

func (s *LogonStateTestSuite) TestFixMsgInLogonInitiateLogon() {
	s.session.initiateLogon = true
	s.store.IncrNextSenderMsgSeqNum()
	s.messageFactory.seqNum = 1
	s.store.IncrNextTargetMsgSeqNum()

	logon := s.Logon()
	logon.Body.SetField(tagHeartBtInt, FIXInt(32))

	s.mockApp.On("FromAdmin").Return(nil)
	s.mockApp.On("OnLogon")
	s.FixMsgIn(s.session, logon)

	s.mockApp.AssertExpectations(s.T())
	s.State(inSession{})

	s.NextTargetMsgSeqNum(3)
	s.NextSenderMsgSeqNum(2)
}