import http from 'k6/http';
import { check } from 'k6';

// 재고 20개인 상품에 100개의 동시 요청을 쏴서, Saga 전체 흐름(order-service →
// inventory-service Redis Lua 차감)을 거쳐도 오버셀이 발생하지 않는지 인프라
// 레벨에서 검증한다. (단위 테스트인 InventoryServiceConcurrencyTest와 같은
// 주장을, 실제 배포된 HTTP API를 통해 end-to-end로 다시 증명하는 것이 목적)

export const options = {
  scenarios: {
    burst: {
      executor: 'shared-iterations',
      vus: 50,          // 동시 가상 사용자 50명
      iterations: 100,  // 총 100개 요청을 50명이 나눠서 거의 동시에 발사
      maxDuration: '30s',
    },
  },
};

const PRODUCT_ID = __ENV.PRODUCT_ID || 'k6-test-product';
// kubectl port-forward는 단일 터널이라 동시 연결 50개를 못 버티고 dropped됨.
// OrbStack은 ClusterIP를 호스트에서 직접 라우팅하므로 그걸 우회해서 직접 호출.
const BASE_URL = __ENV.BASE_URL || 'http://localhost:8081';

export default function () {
  const payload = JSON.stringify({
    userId: `k6-user-${__VU}-${__ITER}`,
    productId: PRODUCT_ID,
    quantity: 1,
  });

  const res = http.post(`${BASE_URL}/orders`, payload, {
    headers: { 'Content-Type': 'application/json' },
  });

  check(res, {
    'order accepted (202)': (r) => r.status === 202,
  });
}
