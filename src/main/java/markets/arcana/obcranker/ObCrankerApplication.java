//
//      Markets that are being cranked:
//      SOL/USDC = CFSMrBssNG8Ud1edW59jNLnq2cwrQ9uY5cM3wXmqRJj3
//      KMNO/USDC = 2ekKD6GQy9CPqyqZyFdERr14JcjD5QcJj7DbFfW23k4W
//      BONK/USDC = Gio5iGZF9YVvhX6vwW3fZEfnPhtafseapaseGbAoiH9D
//      BONK/SOL = DBSZ24hqXS5o8djunrTzBsJUb1P8ZvBs1nng5rmZKsJt
//      JUP/USDC = 7iDUNFiwpGjgFW5JmAjhVGXWdBfBXkc9ibFnxUrPNHjM
//      MPLX/USDC = Gudvr1FPgxKfnMoEEBXDgXWzmoavTY7nGC9TcdM4s3SP
//      ETH/USDC = 4fCi5tr3CuBNgMZcaEUXTwm6f346Z5XGBNXB7d6jipFD
//      JTO/USDC = FSH2ModacQupfzjBXbNmKW4favFhjBN6xbms8CRqe6PV
//      DRFT/USDC = D8BPZXCYvVBkXR5NAoDnuzjFGuF2kFKWyfEUtZbmjRg7
//      DRFT/SOL = CGqQYE1SGduEx4vKBfmJQ7Yj4tAyDeV932RuaaCq7Xkz 
//      
package markets.arcana.obcranker;

import com.google.common.io.Resources;
import com.mmorrell.openbook.manager.OpenBookManager;
import com.mmorrell.openbook.model.OpenBookMarket;
import lombok.extern.slf4j.Slf4j;
import org.p2p.solanaj.core.Account;
import org.p2p.solanaj.core.PublicKey;
import org.p2p.solanaj.rpc.RpcClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
@Slf4j
public class ObCrankerApplication {

    @Value("${application.endpoint}")
    private String endpoint;

    @Value("${application.privateKey}")
    private String privateKeyFileName;

    @Value("${application.solUsdcPriorityFee}")
    private int solUsdcPriorityFee;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(8);
    public static void main(String[] args) {
        SpringApplication.run(ObCrankerApplication.class, args);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void crankEventHeapLoop() {
        OpenBookManager manager = new OpenBookManager(new RpcClient(endpoint, 10));

        Account tradingAccount = null;
        try {
            tradingAccount = Account.fromJson(
                    Resources.toString(Resources.getResource(privateKeyFileName), Charset.defaultCharset()));
        } catch (IOException e) {
            log.error("Error reading private key: {}", e.getMessage());
        }
        Account finalTradingAccount = tradingAccount;

        log.info("Crank wallet: {}", finalTradingAccount.getPublicKey().toBase58());
        log.info("RPC: {}", endpoint);

        // Cranking SOL/USDC
        scheduler.scheduleAtFixedRate(() -> {
            try {
                PublicKey marketId = PublicKey.valueOf("CFSMrBssNG8Ud1edW59jNLnq2cwrQ9uY5cM3wXmqRJj3");
                Optional<String> transactionId = manager.consumeEvents(
                        finalTradingAccount,
                        marketId,
                        8,
                        "Cranked by QuanDeFi \uD83D\uDC38",
                        solUsdcPriorityFee
                );

                if (transactionId.isPresent()) {
                    log.info("Cranked SOL/USDC events: {}", transactionId.get());
                } else {
                    log.info("No events found for SOL/USDC.");
                }
            } catch (Exception ex) {
                log.error("Error cranking SOL/USDC: {}", ex.getMessage(), ex);
            }
        }, 0, 1000, TimeUnit.MILLISECONDS); // 1s frequency

        // Cranking KMNO/USDC
        scheduler.scheduleAtFixedRate(() -> {
            try {
                PublicKey marketId = PublicKey.valueOf("2ekKD6GQy9CPqyqZyFdERr14JcjD5QcJj7DbFfW23k4W");
                Optional<String> transactionId = manager.consumeEvents(
                        finalTradingAccount,
                        marketId,
                        8,
                        "Cranked by QuanDeFi \uD83D\uDC38",
                        solUsdcPriorityFee
                );

                if (transactionId.isPresent()) {
                    log.info("Cranked KMNO/USDC events: {}", transactionId.get());
                } else {
                    log.info("No events found for KMNO/USDC.");
                }
            } catch (Exception ex) {
                log.error("Error cranking KMNO/USDC: {}", ex.getMessage(), ex);
            }
        }, 0, 1000, TimeUnit.MILLISECONDS);  // 1s frequency

        // Cranking BONK/USDC
        scheduler.scheduleAtFixedRate(() -> {
            try {
                PublicKey marketId = PublicKey.valueOf("Gio5iGZF9YVvhX6vwW3fZEfnPhtafseapaseGbAoiH9D");
                Optional<String> transactionId = manager.consumeEvents(
                        finalTradingAccount,
                        marketId,
                        8,
                        "Cranked by QuanDeFi \uD83D\uDC38",
                        solUsdcPriorityFee
                );

                if (transactionId.isPresent()) {
                    log.info("Cranked BONK/USDC events: {}", transactionId.get());
                } else {
                    log.info("No events found for BONK/USDC.");
                }
            } catch (Exception ex) {
                log.error("Error cranking BONK/USDC: {}", ex.getMessage(), ex);
            }
        }, 0, 1000, TimeUnit.MILLISECONDS);  // 1s frequency

        // Cranking BONK/SOL
        scheduler.scheduleAtFixedRate(() -> {
            try {
                PublicKey marketId = PublicKey.valueOf("DBSZ24hqXS5o8djunrTzBsJUb1P8ZvBs1nng5rmZKsJt");
                Optional<String> transactionId = manager.consumeEvents(
                        finalTradingAccount,
                        marketId,
                        8,
                        "Cranked by QuanDeFi \uD83D\uDC38",
                        solUsdcPriorityFee
                );

                if (transactionId.isPresent()) {
                    log.info("Cranked BONK/SOL events: {}", transactionId.get());
                } else {
                    log.info("No events found for BONK/SOL.");
                }
            } catch (Exception ex) {
                log.error("Error cranking BONK/SOL: {}", ex.getMessage(), ex);
            }
        }, 0, 1000, TimeUnit.MILLISECONDS);  // 1s frequency

        // Cranking JUP/USDC
        scheduler.scheduleAtFixedRate(() -> {
            try {
                PublicKey marketId = PublicKey.valueOf("7iDUNFiwpGjgFW5JmAjhVGXWdBfBXkc9ibFnxUrPNHjM");
                Optional<String> transactionId = manager.consumeEvents(
                        finalTradingAccount,
                        marketId,
                        8,
                        "Cranked by QuanDeFi \uD83D\uDC38",
                        solUsdcPriorityFee
                );
    
                if (transactionId.isPresent()) {
                    log.info("Cranked JUP/USDC events: {}", transactionId.get());
                } else {
                    log.info("No events found for JUP/USDC.");
                }
            } catch (Exception ex) {
                log.error("Error cranking JUP/USDC: {}", ex.getMessage(), ex);
            }
        }, 0, 1000, TimeUnit.MILLISECONDS);  // 1s frequency

        // Cranking MPLX/USDC
        scheduler.scheduleAtFixedRate(() -> {
            try {
                PublicKey marketId = PublicKey.valueOf("Gudvr1FPgxKfnMoEEBXDgXWzmoavTY7nGC9TcdM4s3SP");
                Optional<String> transactionId = manager.consumeEvents(
                        finalTradingAccount,
                        marketId,
                        8,
                        "Cranked by QuanDeFi \uD83D\uDC38",
                        solUsdcPriorityFee
                );
    
                if (transactionId.isPresent()) {
                    log.info("Cranked MPLX/USDC events: {}", transactionId.get());
                } else {
                    log.info("No events found for MPLX/USDC.");
                }
            } catch (Exception ex) {
                log.error("Error cranking MPLX/USDC: {}", ex.getMessage(), ex);
            }
        }, 0, 1000, TimeUnit.MILLISECONDS);  // 1s frequency

        // Cranking ETH/USDC
        scheduler.scheduleAtFixedRate(() -> {
            try {
                PublicKey marketId = PublicKey.valueOf("4fCi5tr3CuBNgMZcaEUXTwm6f346Z5XGBNXB7d6jipFD");
                Optional<String> transactionId = manager.consumeEvents(
                        finalTradingAccount,
                        marketId,
                        8,
                        "Cranked by QuanDeFi \uD83D\uDC38",
                        solUsdcPriorityFee
                );
    
                if (transactionId.isPresent()) {
                    log.info("Cranked ETH/USDC events: {}", transactionId.get());
                } else {
                    log.info("No events found for ETH/USDC.");
                }
            } catch (Exception ex) {
                log.error("Error cranking ETH/USDC: {}", ex.getMessage(), ex);
            }
        }, 0, 1000, TimeUnit.MILLISECONDS);  // 1s frequency

        // Cranking JTO/USDC
        scheduler.scheduleAtFixedRate(() -> {
            try {
                PublicKey marketId = PublicKey.valueOf("FSH2ModacQupfzjBXbNmKW4favFhjBN6xbms8CRqe6PV");
                Optional<String> transactionId = manager.consumeEvents(
                        finalTradingAccount,
                        marketId,
                        8,
                        "Cranked by QuanDeFi \uD83D\uDC38",
                        solUsdcPriorityFee
                );
    
                if (transactionId.isPresent()) {
                    log.info("Cranked JTO/USDC events: {}", transactionId.get());
                } else {
                    log.info("No events found for JTO/USDC.");
                }
            } catch (Exception ex) {
                log.error("Error cranking JTO/USDC: {}", ex.getMessage(), ex);
            }
        }, 0, 1000, TimeUnit.MILLISECONDS);  // 1s frequency

        // Cranking DRFT/USDC
        scheduler.scheduleAtFixedRate(() -> {
            try {
                PublicKey marketId = PublicKey.valueOf("D8BPZXCYvVBkXR5NAoDnuzjFGuF2kFKWyfEUtZbmjRg7");
                Optional<String> transactionId = manager.consumeEvents(
                        finalTradingAccount,
                        marketId,
                        8,
                        "Cranked by QuanDeFi \uD83D\uDC38",
                        solUsdcPriorityFee
                );
    
                if (transactionId.isPresent()) {
                    log.info("Cranked DRFT/USDC events: {}", transactionId.get());
                } else {
                    log.info("No events found for DRFT/USDC.");
                }
            } catch (Exception ex) {
                log.error("Error cranking DRFT/USDC: {}", ex.getMessage(), ex);
            }
        }, 0, 1000, TimeUnit.MILLISECONDS);  // 1s frequency    

        // Cranking DRFT/SOL
        scheduler.scheduleAtFixedRate(() -> {
            try {
                PublicKey marketId = PublicKey.valueOf("CGqQYE1SGduEx4vKBfmJQ7Yj4tAyDeV932RuaaCq7Xkz");
                Optional<String> transactionId = manager.consumeEvents(
                        finalTradingAccount,
                        marketId,
                        8,
                        "Cranked by QuanDeFi \uD83D\uDC38",
                        solUsdcPriorityFee
                );
    
                if (transactionId.isPresent()) {
                    log.info("Cranked DRFT/SOL events: {}", transactionId.get());
                } else {
                    log.info("No events found for DRFT/SOL.");
                }
            } catch (Exception ex) {
                log.error("Error cranking DRFT/SOL: {}", ex.getMessage(), ex);
            }
        }, 0, 1000, TimeUnit.MILLISECONDS);  // 1s frequency    

        // Cranking all other markets
        scheduler.scheduleAtFixedRate(() -> {
            try {
                manager.cacheMarkets();
                for (OpenBookMarket market : manager.getOpenBookMarkets()) {
                    Optional<String> transactionId = Optional.empty();
                    try {
                        transactionId = manager.consumeEvents(
                                finalTradingAccount,
                                market.getMarketId(),
                                8,
                                "Cranked by QuanDeFi \uD83D\uDC38"
                        );
                    } catch (Exception ex) {
                        log.error(
                                "Error cranking market [{}]: {}",
                                market.getMarketId().toBase58(),
                                ex.getMessage(),
                                ex
                        );
                    }

                    if (transactionId.isPresent()) {
                        log.info("Cranked events [{}]: {}", market.getName(), transactionId.get());
                    } else {
                        log.info("No events found to consume.");
                    }
                    try {
                        Thread.sleep(250);
                    } catch (InterruptedException e) {
                        log.error("Error consuming: {}", e.getMessage());
                    }
                }
            } catch (Exception ex) {
                log.error("Error caching/cranking markets: {}", ex.getMessage(), ex);
            }
        }, 0, 30, TimeUnit.SECONDS);
    }
}
