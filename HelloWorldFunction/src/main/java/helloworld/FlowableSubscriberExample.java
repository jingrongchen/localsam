package helloworld;

import io.reactivex.rxjava3.core.BackpressureStrategy;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.FlowableEmitter;
import io.reactivex.rxjava3.core.FlowableOnSubscribe;
import io.reactivex.rxjava3.core.FlowableSubscriber;
import org.reactivestreams.Subscription;
import io.reactivex.rxjava3.schedulers.Schedulers;

public class FlowableSubscriberExample {

    public static void main(String[] args) {
        // 创建 Publisher
        Flowable<Integer> publisher = Flowable.create(new FlowableOnSubscribe<Integer>() {
            @Override
            public void subscribe(FlowableEmitter<Integer> emitter) throws Exception {
                for (int i = 0; i < 10; i++) {
                    if (emitter.isCancelled()) {
                        return; // 如果被取消订阅，则结束数据发送
                    }

                    // 模拟耗时操作
                    Thread.sleep(500);

                    // 发送数据
                    emitter.onNext(i);
                }
                // 完成数据发送
                emitter.onComplete();
            }
        }, BackpressureStrategy.BUFFER).onBackpressureBuffer(1024).subscribeOn(Schedulers.newThread());

        // 创建 Subscriber
        FlowableSubscriber<Integer> subscriber = new FlowableSubscriber<Integer>() {
            private Subscription subscription;

            @Override
            public void onSubscribe(Subscription s) {
                subscription = s;
                // 请求第一个数据
                subscription.request(1);
                System.out.println("onSubscribe");
            }

            @Override
            public void onNext(Integer integer) {
                // 处理接收到的数据
                System.out.println("onNext: " + integer);
                // 请求下一个数据
                subscription.request(1);
            }

            @Override
            public void onError(Throwable throwable) {
                throwable.printStackTrace();
            }

            @Override
            public void onComplete() {
                System.out.println("onComplete on subscriber!");
            }
        };

        // 订阅 Flowable
        publisher.observeOn(Schedulers.newThread()).subscribe(subscriber);

        // 等待一段时间，以便观察结果
        try {
            Thread.sleep(6000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
