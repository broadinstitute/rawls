import http from 'k6/http';
import { sleep, check } from 'k6';

export const options = {
    vus: 3,
    duration: '05s',
    thresholds: {
        http_req_duration: ['p(95)<500', 'p(99)<1500'],
    },
};

export default function() {
    let res = http.get('https://bvdp-saturn-staging.appspot.com/config.json');
    check(res, { "status is 200": (res) => res.status === 200 });
    sleep(1);
}
