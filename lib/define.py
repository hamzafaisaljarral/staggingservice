# -*- encoding:utf8 -*-
FIAT = 'USDT'
R_DIGIT = 2  # Integer연산을 할때 반올림을 할수 있도록 추가로 0을 붙여서 계산을 하기 위한 단위
NOTICE_TYPE = {
    'REG_STOP_LIMIT': 1,
    'REG_STOP_MARKET': 2,
    'REG_LIMIT_STOP': 3,
    'REG_LIMIT': 4,

    'MATCH_LIMIT': 5,
    'MATCH_MARKET': 6,
    'MATCH_MARKET_STOP': 7,

    'CANCEL_STOP_LIMIT': 8,
    'CANCEL_STOP_MARKET': 9,
    'CANCEL_LIMIT': 10,

    'DEPOSIT_COMPLETE': 21,
    'WITHDRAW_REQUEST': 22,
    'WITHDRAW_CANCEL': 23,
    'WITHDRAW_DENY': 24,
    'WITHDRAW_COMPLETE': 25,
    'WITHDRAW_FAIL': 26,

    # 'MODIFY_LIMIT':11,
}
pNOTICE_TYPE = {v: k for (k, v) in NOTICE_TYPE.items()}
RESPONSE_TYPE = {
    'SYSTEM': 0,
    'ORDER': 1,
}
pRESPONSE_TYPE = {v: k for (k, v) in RESPONSE_TYPE.items()}
RESPONSE_CODE = {
    # SYSTEM
    'JSON_ERROR': 1000,  # request가 json이 아닌 경우,
    'INVALID_PARAM': 1001,  # request의 parameter가 잘못 들어온 경우, 'key' 값을 함께 보냄

    # ORDER
    'LIMIT_REGIST': 0,  # 지정가 주문 등록
    'MATCH': 1,  # MATCH
    'CANCEL': 2,  # LIMIT 주문 정상 취소처리됨
    # 'MODIFY': 3,  # LIMIT 주문 정상 수정처리됨
    'STOP_REGIST': 4,  # 예약 주문 등록
    'STOP_CANCEL': 5,  # 예약 주문 취소

    'DEPOSIT_COMPLETE': 6,  # 입금 완료
    'WITHDRAW_REQUEST': 7,  # 출금 요청
    'WITHDRAW_CANCEL': 8,  # 출금 취소
    'WITHDRAW_DENY': 9,  # 출금 반려
    'WITHDRAW_COMPLETE': 10,  # 출금 완료
    'WITHDRAW_FAIL': 11,  # 출금 실패

    'NOT_ENOUGH_BALANCE': 5000,  # balance가 없음
    # 'NOT_ALLOW_FUNDS':5001, # 입력한 funds로 거래할수 없음                                      # MatchEngin에서 발생한 FAIL
    'NOT_FOUND_ORDER_ID': 5002,  # 입력한 order_id를 확인할수 없다.                               # MatchEngin에서 발생한 FAIL
    # 'NOT_FOUND_ORDER_ID(AUTH)':5003, # 입력한 order_id를 확인할수 없다. (member_id까지 비교)    # MatchEngin에서 발생한 FAIL
    'NOT_ALLOW_PRICE': 5004,  # Modify 불가능한 price                                             # MatchEngin에서 발생한 FAIL
    'NOT_ALLOW_STOP_PRICE': 5005,  # stop 불가능한 price                                          # PreDaemon에서 발생
    'NOT_ACCUMULATE_SIZE': 5006,  # 해당 order와 match할 maker가 없다, 즉 매칭된 size가 하나도 없다.
    'NOT_ALLOW_ORDER': 5007,  # price와 size로 계산한 거래대금이 qcu보다 작다. price / size < 10**qcu  또는 온전치 못한 주문이 들어왔을때

    'INTERNAL_ERROR': 5008,  # member_id 확인이 가능한 경우, 내부 에러

    # 'NOT_ALLOW_PRODUCT':5100, # products.status가 0일때
    # 'NOT_ALLOW_PRODUCT_ORDER_TYPE':5101, # product_trade_deny에 등록되어 있을때
}
pRESPONSE_CODE = {v: k for (k, v) in RESPONSE_CODE.items()}
INCOMING = {
    # 'META':0,
    'WEB': 1,
    'MOBILE': 2,
    'INSIDE': 3, }
pINCOMING = {v: k for (k, v) in INCOMING.items()}
ORDER_TYPE = {  # 여기서 주석처리하면 선처리에서 해당 Order를 받지 앖고, INVALID_PARAM error를 발생시킨다.
    'MARKET': 0,
    'LIMIT': 1,
    'STOP': 2,
    'CANCEL': 3,
    # 'MODIFY':4,
    'STOPCANCEL': 5,
}
pORDER_TYPE = {v: k for (k, v) in ORDER_TYPE.items()}
POLICY = {
    'GTC': 0,
    'GTT': 1,
    # 'IOC':2, # 임시로 IOC, FOK 주문 차단 # PostProcess에서 작업 무시
    # 'FOK':3,
}
pPOLICY = {v: k for (k, v) in POLICY.items()}
SIDE = {
    'BUY': 0,
    'SELL': 1, }
pSIDE = {v: k for (k, v) in SIDE.items()}
AMOUNT_TYPE = {
    'SIZE': 0,  # Limit Buy, Sell, Market Sell
    'FUNDS': 1,  # Market Buy
}
pAMOUNT_TYPE = {v: k for (k, v) in AMOUNT_TYPE.items()}
HISTORY_TYPE = {
    'OPEN': 0,
    'STOP': 1,
    # 'MODIFY':2,
}
pHISTORY_TYPE = {v: k for (k, v) in HISTORY_TYPE.items()}
DETAIL_TYPE = {
    'MATCH': 0,
    'CANCEL': 1,
    'STOP': 2,
}
pDETAIL_TYPE = {v: k for (k, v) in DETAIL_TYPE.items()}

CANCEL_TYPE = {
    'SIGNAL': 0,
    'TIME': 1,
    'IOC': 2,
    'FOK': 3, }
pCANCEL_TYPE = {v: k for (k, v) in CANCEL_TYPE.items()}

INOUT = {
    'IN': 0,
    'OUT': 1,
}
pINOUT = {v: k for (k, v) in INOUT.items()}

WALLET_DETAIL_INOUT = {
    'IN': 0,
    'OUT': 1,
}
pWALLET_DETAIL_INOUT = {v: k for (k, v) in WALLET_DETAIL_INOUT.items()}

WALLET_DETAIL_SIDE = {
    'BUY': 0,
    'SELL': 1,
    'DEPOSIT': 2,
    'WITHDRAW': 3,
}
pWALLET_DETAIL_SIDE = {v: k for (k, v) in WALLET_DETAIL_SIDE.items()}

EMAILTYPE = {
    'Custom': 0,
    'EmailVerification': 1,
    'PasswordReset': 2,
    'DepositComplete': 3,
    'WithdrawRequest': 4,
    'WithdrawCancel': 5,
    'WithdrawDeny': 6,
    'WithdrawComplete': 7,
    'WithdrawFail': 8,
    'QnaAnswer': 9,
}
pEMAILTYPE = {v: k for (k, v) in EMAILTYPE.items()}